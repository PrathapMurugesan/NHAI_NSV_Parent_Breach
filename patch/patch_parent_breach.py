#!/usr/bin/env python3
"""
Patch: apply the "parent / lane breach follows the 'as per CA' children" rule to UPCs that are already processed.

Rule (same as src/hierarchy/parent_builder.py and lane_builder.py): a LHS / RHS 1000m parent of roughness and
rutting - and a lane's 1000m record in master_<defect>_1000m_lane (roughness, rutting, ravelling, cracking) - is a
breach when its value is above the limit OR when any one "as per CA" child (master_<defect>_as_per_ca, the child at
the CA reporting interval; for ravelling master_ravelling) of that side / lane in those 1000m is a breach. The 10m
rows are not used. IRC is not touched. The extraction is NOT run again: the patch reads the UPC's existing
parquets, changes only the parent and lane tables and writes them back.

Per parent / lane row that becomes a breach (a row that is already a breach never changes, so the patch can run twice):
    breach_flag = 1, defect_id = parent_id, road_condition = 'Bad', rating_ca = the 'Poor' band of the defect
    (its own average could still rate Good / Very Good), affected_length_ca / affected_length = total_length
    (parents) or length_m (lanes)
    measures JSON gets breach_ca / breach_from_child_ca (and patched_parent_breach)
Defect IDs: a breach row that already has a display_defect_id keeps it; a breach row that has none gets a new
D/<ddmmyy>/N/<n> continuing the UPC's counter (highest counter of any of its tables + 1). So the lane tables, which
had no IDs, are numbered too. No existing ID of any table is changed, so the child rows (and the image upload log)
keep their IDs.
A UPC processed before the lane tables existed has no master_<defect>_1000m_lane file: it is reported as skipped
(run that UPC again to get the tables).

Nothing is written unless --apply is given. With --apply every changed parquet is first copied to
<silver-root>/_patch_backup/parent_breach_<time>/Zone=../Phase=../Cycle=../UPC=../dlm_kpi_new/ .

Which UPCs:
    --all                         every UPC found under the silver root (Zone=*/Phase=*/Cycle=*/UPC=*)
    --zone B --phase 2 --cycle 1 --upc N_07024_01001_GJ        one UPC
    --upc-file batches/upc_list1.txt                           lines "UPC ZONE PHASE CYCLE" (# = comment)
    --zone A --all                                             --zone / --phase / --cycle also narrow --all

    python3 patch/patch_parent_breach.py --all                    dry run, report only
    python3 patch/patch_parent_breach.py --all --apply            patch them
Report: patch/logs/parent_breach_<time>.csv (and <silver-root>/_patch_logs/ with --apply).
Log:    patch/logs/parent_breach_<time>.log - every UPC, table, count and warning (also copied to _patch_logs/ with --apply).
DQ:     patch/logs/parent_breach_dq_<time>.csv - data-quality checks P1-P7 per patched table (layout of the extraction's
        validation report; the same file goes to _patch_logs/ with --apply). With --apply a table that fails its read-back
        check is restored from its backup and reported as ERROR. The exit code is 1 on any ERROR or DQ FAIL.
"""
import argparse
import csv
import io
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import numpy as np
import pandas as pd

from src.common.constants import PARENT_LENGTH_M, TABLE_SPEC, lane_1000m_table, normalize_lane
from src.common.fs_utils import list_dir, path_exists, read_file_bytes, write_file_bytes
from src.common.logger import setup_logger
from src.hierarchy.lane_builder import assign_missing_display_ids, child_breach_lane_bins
from src.hierarchy.parent_builder import child_breach_bins
from src.processing.ratings import poor_rating_label

DEFAULT_SILVER_ROOT = "s3://dhms-new-stgae/DHMS/NSV/Silver"
DEFECTS = ("roughness", "rutting")             # the defects with LHS / RHS parents
LANE_DEFECTS = ("roughness", "rutting", "ravelling", "cracking")      # the defects with a lane-wise 1000m table
ID_RE = re.compile(r"^D/(\d{6})/N/(\d{5})(?:/\d{4})?$")
logger = setup_logger("dlm_pipeline.patch")
REPORT_COLS = ["zone", "phase", "cycle", "upc", "table", "parents", "to_ca_breach", "new_display_ids", "status", "note"]


def _read(path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(read_file_bytes(path)), columns=columns)


def _write(df: pd.DataFrame, path: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    write_file_bytes(path, buf.getvalue())


def _up(a: str) -> str:
    return a.rstrip("/")


def _val(entry: str) -> str:
    return entry.split("=", 1)[1] if "=" in entry else entry


def discover(root: str, zone=None, phase=None, cycle=None) -> List[Tuple[str, str, str, str]]:
    """[(zone, phase, cycle, upc)] of every UPC folder under the silver root."""
    found = []
    for z in (e for e in list_dir(root) if e.startswith("Zone=")):
        if zone and _val(z) != str(zone):
            continue
        for p in (e for e in list_dir(f"{root}/{z}") if e.startswith("Phase=")):
            if phase and _val(p) != str(phase):
                continue
            for c in (e for e in list_dir(f"{root}/{z}/{p}") if e.startswith("Cycle=")):
                if cycle and _val(c) != str(cycle):
                    continue
                for u in (e for e in list_dir(f"{root}/{z}/{p}/{c}") if e.startswith("UPC=")):
                    found.append((_val(z), _val(p), _val(c), _val(u)))
    return sorted(found)


def from_file(path: str) -> List[Tuple[str, str, str, str]]:
    out = []
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        upc, zone, phase, cycle = line.replace(",", " ").split()[:4]
        out.append((zone, phase, cycle, upc))
    return out


def _parent_bin(df: pd.DataFrame) -> pd.Series:
    if "parent_start_chainage" in df.columns and pd.to_numeric(df["parent_start_chainage"], errors="coerce").notna().all():
        return pd.to_numeric(df["parent_start_chainage"]).astype(int)
    lo = np.minimum(pd.to_numeric(df["start_chainage"], errors="coerce"), pd.to_numeric(df["end_chainage"], errors="coerce"))
    return (np.floor(lo / PARENT_LENGTH_M) * PARENT_LENGTH_M).astype(int)


def _counter_state(folder: str) -> Tuple[int, Optional[str], set]:
    """(highest top counter, most used ddmmyy, every display_defect_id) over every table of the UPC."""
    top, dates, seen = 0, {}, set()
    for name in list_dir(folder):
        if not name.endswith(".parquet"):
            continue
        try:
            ids = _read(f"{folder}/{name}", ["display_defect_id"])["display_defect_id"].dropna().astype(str)
        except Exception:
            continue                                  # table without that column
        seen.update(ids)
        for v in ids:
            m = ID_RE.match(v)
            if m:
                top = max(top, int(m.group(2)))
                dates[m.group(1)] = dates.get(m.group(1), 0) + 1
    return top, (max(dates, key=dates.get) if dates else None), seen


def _survey_date(parent: pd.DataFrame) -> Optional[str]:
    for col in ("survey_end_date", "survey_start_date", "run_date", "detected_on"):
        if col in parent.columns:
            d = pd.to_datetime(parent[col], errors="coerce").dropna()
            if len(d):
                return d.iloc[0].strftime("%d%m%y")
    return None


def _patch_measures(raw, stamp: str):
    try:
        m = json.loads(raw) if isinstance(raw, str) else None
    except Exception:
        m = None
    if not isinstance(m, dict):
        return raw
    m.update(breach_ca=1, breach_from_value_ca=0, breach_from_child_ca=1)
    m["patched_parent_breach"] = stamp
    return json.dumps(m)


def _ca_child_table(defect: str) -> str:
    """The defect's "as per CA" child table (the child at the CA reporting interval)."""
    return next(t for t, kind in TABLE_SPEC[defect]["children"] if kind == "ca")


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce") if col in df.columns else pd.Series(np.nan, index=df.index)


def _needs_id(df: pd.DataFrame) -> pd.Series:
    """Breach rows that have no display_defect_id yet."""
    has = pd.Series(False, index=df.index)
    if "display_defect_id" in df.columns:
        has = df["display_defect_id"].notna() & (df["display_defect_id"].astype(str) != "")
    return (_num(df, "breach_flag").fillna(0).astype(int) == 1) & ~has


def _prep(folder: str, base: Dict, kind: str, d: str, rows: List[Dict], stamp: str) -> Optional[Dict]:
    """
    Load one 'parent' (LHS / RHS) or 'lane' table and the defect's "as per CA" child table, and apply the breach
    rule in memory. Returns a work item, or None when the table cannot be patched (a 'skipped' row is added).
    """
    child_table = _ca_child_table(d)
    if kind == "parent":
        table = f"master_{d}_parent"
        missing = "parent table not found"
    else:
        table = lane_1000m_table(d)
        missing = "no lane table in this UPC (processed before the lane tables existed) - run it again"
    path, c_path = f"{folder}/{table}.parquet", f"{folder}/{child_table}.parquet"
    if not path_exists(path):
        rows.append({**base, "table": table, "status": "skipped", "note": missing})
        return None
    df = _read(path)
    if df.empty:
        rows.append({**base, "table": table, "parents": 0, "status": "skipped", "note": "table is empty"})
        return None
    if not path_exists(c_path):
        rows.append({**base, "table": table, "parents": len(df), "status": "skipped",
                     "note": f"{child_table} not found - cannot see the children"})
        return None
    child = _read(c_path)

    bins = _parent_bin(df)
    if kind == "parent":
        hits = child_breach_bins(child, "As per CA", PARENT_LENGTH_M)
        keys = list(zip(df["side"].astype(str), bins))
        length = _num(df, "total_length").fillna(_num(df, "length_m"))
    else:
        hits = child_breach_lane_bins(child, "As per CA", PARENT_LENGTH_M)
        keys = list(zip(df["lane"].apply(normalize_lane), bins))
        length = _num(df, "length_m").fillna((_num(df, "end_chainage") - _num(df, "start_chainage")).abs())
    child_ca = pd.Series([(k, int(b)) in hits for k, b in keys], index=df.index)
    was_ca = _num(df, "breach_flag").fillna(0).astype(int) == 1
    to_ca = child_ca & ~was_ca

    out = df.copy()
    no_thr = 0                                                  # flagged rows whose 'Poor' rating could not be named
    if to_ca.any():
        out.loc[to_ca, "breach_flag"] = 1
        out.loc[to_ca, "defect_id"] = out.loc[to_ca, "parent_id"]
        out.loc[to_ca, "road_condition"] = "Bad"
        out.loc[to_ca, "affected_length_ca"] = length[to_ca]
        out.loc[to_ca, "affected_length"] = length[to_ca]
        thr = _num(df, "threshold_value")                       # the CA limit of the row -> its 'Poor' rating label
        for i in out.index[to_ca]:
            poor = poor_rating_label(d, thr[i])
            if poor:
                out.at[i, "rating_ca"] = poor
            else:
                no_thr += 1
        if "measures" in out.columns:
            for i in out.index[to_ca]:
                out.at[i, "measures"] = _patch_measures(out.at[i, "measures"], stamp)
    note = f"rating_ca not changed on {no_thr} row(s): threshold_value is empty" if no_thr else ""
    return dict(table=table, path=path, df=df, out=out, to_ca=to_ca, need=_needs_id(out), note=note,
                defect=d, kind=kind, child_ca=child_ca)



# ----------------------------------------------------------------------------------------------------------
# Data quality (DQ) checks - same layout as the extraction's validation report (src/validation/validator.py):
# section, check, scope, checked, failed, status (PASS / FAIL / WARN / SKIP), detail
# ----------------------------------------------------------------------------------------------------------
def _dq(rows: List[Dict], section: str, check: str, scope: str, checked: int, failed: int,
        detail: str = "", level: str = "FAIL") -> None:
    status = "SKIP" if checked == 0 else ("PASS" if failed == 0 else level)
    rows.append(dict(section=section, check=check, scope=scope, checked=int(checked), failed=int(failed),
                     status=status, detail=str(detail)[:500]))


def _same(a: pd.Series, b: pd.Series) -> pd.Series:
    """Element-wise equality where two empty values count as equal."""
    return (a == b) | (a.isna() & b.isna())


def dq_checks(it: Dict, out: pd.DataFrame, n_ids: int, existing_ids: set, scope: str) -> List[Dict]:
    """
    DQ of one patched table (before = it['df'], after = out):
      P1 rows           row count and key columns (id, parent_id, chainage, side, lane) are unchanged
      P2 flags          no breach_flag went 1 -> 0; every record with a breaching "as per CA" child is a breach
      P3 condition      a record that became a breach through a child is Bad / Poor (Poor when it can be named)
      P4 lengths        every breach record has affected_length_ca > 0 and a defect_id
      P5 display ids    every breach record has one; the existing IDs are unchanged; the new IDs are unique,
                        well formed and not used by any table of the UPC
      P6 untouched      columns the patch does not own (IRC columns, metric values, ratings other than CA) are unchanged
    """
    rows: List[Dict] = []
    before = it["df"]
    n = len(before)
    d = it["defect"]

    # P1 rows / keys
    _dq(rows, "P1 rows", "row count unchanged", scope, n, int(len(out) != n), f"before {n}, after {len(out)}")
    if len(out) == n:
        keys = [c for c in ("id", "parent_id", "start_chainage", "end_chainage", "side", "lane") if c in before.columns]
        bad = ~pd.concat([_same(before[c], out[c]) for c in keys], axis=1).all(axis=1) if keys else pd.Series(False, index=before.index)
        _dq(rows, "P1 rows", "key columns unchanged", scope, n, int(bad.sum()), ",".join(keys))
    else:
        return rows

    # P2 flags
    was = _num(before, "breach_flag").fillna(0).astype(int)
    now = _num(out, "breach_flag").fillna(0).astype(int)
    _dq(rows, "P2 flags", "no breach flag removed", scope, int((was == 1).sum()), int(((was == 1) & (now == 0)).sum()))
    _dq(rows, "P2 flags", "child breach is a parent / lane breach", scope, int(it["child_ca"].sum()),
        int((it["child_ca"] & (now == 0)).sum()), "a record with a breaching 'as per CA' child must have breach_flag = 1")
    _dq(rows, "P2 flags", "flags changed only by the rule", scope, n,
        int(((now != was) & ~it["to_ca"]).sum()), "a changed flag must be a record whose child breaches")

    # P3 condition / rating of the rows that became a breach
    chg = it["to_ca"]
    k = int(chg.sum())
    _dq(rows, "P3 condition", "child-only breach is Bad", scope, k,
        int((out.loc[chg, "road_condition"].astype(str) != "Bad").sum()) if k else 0)
    if k:
        thr = _num(out, "threshold_value")
        named = chg & thr.notna()
        poor = pd.Series([poor_rating_label(d, thr[i]) for i in out.index[named]], index=out.index[named])
        ok = poor.notna()
        bad = int((out.loc[poor.index[ok], "rating_ca"].astype(str) != poor[ok].astype(str)).sum())
        _dq(rows, "P3 condition", "child-only breach has the Poor rating", scope, int(ok.sum()), bad)
        _dq(rows, "P3 condition", "Poor rating could be named", scope, k, int(k - ok.sum()),
            "threshold_value is empty - rating_ca left as it was", level="WARN")
    else:
        _dq(rows, "P3 condition", "child-only breach has the Poor rating", scope, 0, 0)

    # P4 lengths / defect id of every breach record
    br = now == 1
    _dq(rows, "P4 lengths", "breach has affected_length_ca > 0", scope, int(br.sum()),
        int((_num(out, "affected_length_ca").fillna(0)[br] <= 0).sum()))
    has_def = out["defect_id"].notna() & (out["defect_id"].astype(str) != "") if "defect_id" in out.columns else pd.Series(False, index=out.index)
    _dq(rows, "P4 lengths", "breach has defect_id", scope, int(br.sum()), int((br & ~has_def).sum()))

    # P5 display ids
    old_id = before["display_defect_id"] if "display_defect_id" in before.columns else pd.Series(None, index=before.index, dtype=object)
    new_id = out["display_defect_id"] if "display_defect_id" in out.columns else pd.Series(None, index=out.index, dtype=object)
    had = old_id.notna() & (old_id.astype(str) != "")
    got = new_id.notna() & (new_id.astype(str) != "")
    _dq(rows, "P5 display ids", "breach has display_defect_id", scope, int(br.sum()), int((br & ~got).sum()))
    _dq(rows, "P5 display ids", "existing display_defect_id kept", scope, int(had.sum()),
        int((had & (old_id.astype(str) != new_id.astype(str))).sum()))
    fresh = got & ~had
    ids = new_id[fresh].astype(str)
    _dq(rows, "P5 display ids", "new ids well formed", scope, len(ids), int((~ids.map(lambda v: bool(ID_RE.match(v)))).sum()))
    _dq(rows, "P5 display ids", "new ids unique", scope, len(ids), int(ids.duplicated().sum()))
    _dq(rows, "P5 display ids", "new ids not used before in the UPC", scope, len(ids), int(ids.isin(existing_ids).sum()))
    _dq(rows, "P5 display ids", "ids assigned = ids reported", scope, int(n_ids), int(abs(len(ids) - n_ids)))

    # P6 columns the patch does not own
    owned = {"breach_flag", "defect_id", "road_condition", "rating_ca", "affected_length_ca", "affected_length",
             "measures", "display_defect_id"}
    same_cols = [c for c in before.columns if c not in owned and c in out.columns]
    diff = [c for c in same_cols if not _same(before[c], out[c]).all()]
    _dq(rows, "P6 untouched", "other columns unchanged (IRC, values, ...)", scope, len(same_cols), len(diff), ",".join(diff[:10]))
    return rows


def dq_readback(it: Dict, out: pd.DataFrame, scope: str) -> List[Dict]:
    """After --apply: the parquet that was written is read back and compared with what was meant to be written."""
    rows: List[Dict] = []
    back = _read(it["path"])
    ok_shape = back.shape == out.shape and list(back.columns) == list(out.columns)
    _dq(rows, "P7 readback", "written file has the same shape and columns", scope, 1, int(not ok_shape),
        f"written {back.shape}, expected {out.shape}")
    if ok_shape:
        for col in ("breach_flag", "display_defect_id", "rating_ca", "road_condition"):
            if col in out.columns:
                bad = ~_same(back[col].astype(object), out[col].astype(object))
                _dq(rows, "P7 readback", f"{col} as written", scope, len(out), int(bad.sum()))
    return rows

def patch_upc(root: str, key: Tuple[str, str, str, str], apply: bool, backup_root: str, stamp: str,
              dq: Optional[List[Dict]] = None) -> List[Dict]:
    """
    One UPC: the roughness / rutting LHS-RHS parents and the four lane-wise 1000m tables.
    1. a record whose "as per CA" child breaches becomes a breach (breach flag, condition, rating, lengths)
    2. every breach row that has no display_defect_id gets one (continuing the UPC's counter); an ID that is
       already there is never changed
    """
    zone, phase, cycle, upc = key
    base = dict(zone=zone, phase=phase, cycle=cycle, upc=upc)
    dq = dq if dq is not None else []
    folder = f"{_up(root)}/Zone={zone}/Phase={phase}/Cycle={cycle}/UPC={upc}/dlm_kpi_new"
    rows: List[Dict] = []

    items = [it for it in (_prep(folder, base, "parent", d, rows, stamp) for d in DEFECTS) if it]
    items += [it for it in (_prep(folder, base, "lane", d, rows, stamp) for d in LANE_DEFECTS) if it]

    next_seq, date, existing_ids = 1, None, set()
    if any(it["need"].any() for it in items):
        top, date, existing_ids = _counter_state(folder)
        date = date or _survey_date(items[0]["df"])
        next_seq = top + 1
        if date is None:                                # cannot number: leave those tables alone
            for it in items:
                if it["need"].any():
                    logger.warning("%s %s: display IDs needed but no date found - table left alone", upc, it["table"])
                    rows.append({**base, "table": it["table"], "parents": len(it["df"]), "status": "skipped",
                                 "note": "display IDs needed but the date could not be found"})
            items = [it for it in items if not it["need"].any()]

    for it in items:
        out, n_ids = it["out"], 0
        if it["need"].any():
            out, n_ids = assign_missing_display_ids(out, date, next_seq)
            next_seq += n_ids
        n_ca = int(it["to_ca"].sum())
        rec = {**base, "table": it["table"], "parents": len(it["df"]), "to_ca_breach": n_ca, "new_display_ids": n_ids}
        scope = f"{upc} | {it['table']}"
        checks = dq_checks(it, out, n_ids, existing_ids, scope)
        failed = [c for c in checks if c["status"] == "FAIL"]
        for c in checks:
            if c["status"] in ("FAIL", "WARN"):
                logger.warning("DQ %s %s %s: %s of %s - %s", c["status"], scope, c["check"], c["failed"], c["checked"], c["detail"])
        if it["note"]:
            logger.warning("%s %s: %s", upc, it["table"], it["note"])
        if failed and (n_ca or n_ids):
            # never write a table that fails its own DQ
            logger.error("%s %s: DQ FAIL (%s) - table NOT written", upc, it["table"], "; ".join(c["check"] for c in failed))
            dq += [{**base, **c} for c in checks]
            rows.append({**rec, "status": "ERROR", "note": "DQ failed, nothing written: " + "; ".join(c["check"] for c in failed)})
            continue
        if not (n_ca or n_ids):
            logger.info("%s %s: unchanged (%d records)", upc, it["table"], len(it["df"]))
            rows.append({**rec, "status": "unchanged", "note": it["note"]})
        elif apply:
            rel = it["path"][len(_up(root)) + 1:]
            write_file_bytes(f"{backup_root}/{rel}", read_file_bytes(it["path"]))      # original first
            _write(out, it["path"])
            back = dq_readback(it, out, scope)
            checks += back
            if any(c["status"] == "FAIL" for c in back):
                write_file_bytes(it["path"], read_file_bytes(f"{backup_root}/{rel}"))  # put the original back
                logger.error("%s %s: read-back FAILED - original restored from backup", upc, it["table"])
                rows.append({**rec, "status": "ERROR", "note": f"read-back failed, original restored from {backup_root}/{rel}"})
            else:
                logger.info("%s %s: patched +%d breach +%d IDs (backup %s/%s)", upc, it["table"], n_ca, n_ids, backup_root, rel)
                rows.append({**rec, "status": "patched", "note": "; ".join(filter(None, [it["note"], f"backup: {backup_root}/{rel}"]))})
        else:
            logger.info("%s %s: would patch +%d breach +%d IDs", upc, it["table"], n_ca, n_ids)
            rows.append({**rec, "status": "would patch", "note": "; ".join(filter(None, [it["note"], "dry run (use --apply)"]))})
        dq += [{**base, **c} for c in checks]
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--silver-root", default=os.environ.get("SILVER_S3_ROOT", DEFAULT_SILVER_ROOT))
    ap.add_argument("--all", action="store_true", help="every UPC under the silver root")
    ap.add_argument("--zone")
    ap.add_argument("--phase")
    ap.add_argument("--cycle")
    ap.add_argument("--upc")
    ap.add_argument("--upc-file", help='lines "UPC ZONE PHASE CYCLE"')
    ap.add_argument("--apply", action="store_true", help="write the patched parquets (default: dry run)")
    ap.add_argument("--workers", type=int, default=4, help="UPCs in parallel (default 4)")
    a = ap.parse_args(argv)

    root = _up(a.silver_root)
    if a.upc_file:
        keys = from_file(a.upc_file)
    elif a.upc:
        if not (a.zone and a.phase and a.cycle):
            ap.error("--upc needs --zone, --phase and --cycle")
        keys = [(a.zone, str(a.phase), str(a.cycle), a.upc.replace("/", "_"))]
    elif a.all:
        keys = discover(root, a.zone, a.phase, a.cycle)
    else:
        ap.error("give --all, --upc (with --zone --phase --cycle) or --upc-file")
    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup_root = f"{root}/_patch_backup/parent_breach_{stamp}"
    logs_dir = os.path.join(ROOT, "patch", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"parent_breach_{stamp}.log")
    global logger
    logger = setup_logger("dlm_pipeline.patch", log_file=log_path)
    logger.info("%s  silver root %s  %d UPC(s)  workers %d  log %s", "APPLY" if a.apply else "DRY RUN", root, len(keys),
                a.workers, log_path)
    dq_rows: List[Dict] = []

    def one(key):
        try:
            return patch_upc(root, key, a.apply, backup_root, stamp, dq_rows)
        except Exception as e:                          # one bad UPC never stops the rest
            logger.exception("%s: patch failed", key[3])
            return [dict(zone=key[0], phase=key[1], cycle=key[2], upc=key[3], status="ERROR", note=f"{type(e).__name__}: {e}")]

    report = []
    with ThreadPoolExecutor(max_workers=max(1, a.workers)) as pool:
        for n, rows in enumerate(pool.map(one, keys), start=1):
            report += rows
            hit = [r for r in rows if r.get("status") in ("patched", "would patch")]
            logger.info("[%d/%d] %-20s %s", n, len(keys), keys[n - 1][3],
                        ", ".join(f"{r['table'].replace('master_', '')}: +{r.get('to_ca_breach', 0)} breach, +{r.get('new_display_ids', 0)} IDs"
                                  for r in hit) if hit else "/".join(sorted({r.get('status', '') for r in rows})))

    local = os.path.join(logs_dir, f"parent_breach_{stamp}.csv")
    dq_local = os.path.join(logs_dir, f"parent_breach_dq_{stamp}.csv")
    dq_cols = ["zone", "phase", "cycle", "upc", "section", "check", "scope", "checked", "failed", "status", "detail"]
    with open(dq_local, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, dq_cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(dq_rows, key=lambda r: (r["upc"], r["scope"], r["section"], r["check"])))
    with open(local, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, REPORT_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(report)

    st = pd.DataFrame(report).groupby("status").size() if report else pd.Series(dtype=int)
    logger.info("status per table:\n%s", "\n".join(f"  {n:5}  {s}" for s, n in st.items()))
    logger.info("parent / lane records turned into a breach %d, new display IDs %d",
                sum(int(r.get("to_ca_breach") or 0) for r in report),
                sum(int(r.get("new_display_ids") or 0) for r in report if r.get("status") in ("patched", "would patch")))
    dq_df = pd.DataFrame(dq_rows)
    dq_counts = dq_df["status"].value_counts().to_dict() if not dq_df.empty else {}
    logger.info("DQ checks: %s", dq_counts)
    if not dq_df.empty:
        bad = dq_df[dq_df["status"].isin(["FAIL", "WARN"])].groupby(["status", "section", "check"]).agg(
            tables=("scope", "nunique"), failed=("failed", "sum"))
        if len(bad):
            logger.info("DQ FAIL / WARN by check:\n%s", bad.to_string())
    logger.info("report: %s | dq: %s | log: %s", local, dq_local, log_path)
    if a.apply:
        for src, name in ((local, f"parent_breach_{stamp}.csv"), (dq_local, f"parent_breach_dq_{stamp}.csv"),
                          (log_path, f"parent_breach_{stamp}.log")):
            with open(src, "rb") as fh:
                write_file_bytes(f"{root}/_patch_logs/{name}", fh.read())
        logger.info("copied to %s/_patch_logs/", root)
    else:
        logger.info("dry run - nothing was written. Add --apply to patch.")
    return 1 if (any(r.get("status") == "ERROR" for r in report) or dq_counts.get("FAIL")) else 0


if __name__ == "__main__":
    sys.exit(main())
