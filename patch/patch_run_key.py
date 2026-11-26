#!/usr/bin/env python3
"""
Quick fix: add _P<phase>_C<cycle> to the record IDs of the EXISTING Silver tables - no re-extraction.

Same rule as the extraction (src/output/id_keys.py):
    N_07024_01001_GJ_master_roughness_as_per_ca_000001  ->  N_07024_01001_GJ_P1_C1_master_roughness_as_per_ca_000001
applied to every ID column of every table in <silver>/Zone=<Z>/Phase=<P>/Cycle=<C>/UPC=<UPC>/dlm_kpi_new/, so the parent /
child links stay intact. Already converted tables are left as they are (running it twice changes nothing).

Dry run by default (reports only). --apply rewrites the tables; the originals are kept in <silver>/_idkey_backup/<time>/.
The report goes to patch/logs/run_key_<time>.csv and to <silver>/_patch_logs/.

    python3 patch/patch_run_key.py --input-file surveys.txt --zone B            # dry run
    python3 patch/patch_run_key.py --input-file surveys.txt --zone B --apply
surveys.txt: lines "UPC CYCLE PHASE" (UPC with / or _; header and # comments allowed).
"""
import argparse
import csv
import io
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pandas as pd

from src.common.fs_utils import list_dir, read_file_bytes, write_file_bytes
from src.output.id_keys import ID_COLUMNS, apply_run_key, run_key

DEFAULT_SILVER_ROOT = "s3://dhms-new-stgae/DHMS/NSV/Silver"
COLS = ["zone", "phase", "cycle", "upc", "table", "ids_changed", "rows", "status", "note"]


def parse_surveys(path):
    out = []
    for line in open(path, encoding="utf-8-sig"):
        p = re.split(r"[\s,]+", line.strip())
        if len(p) < 3 or p[0].lower() == "upc" or p[0].startswith("#"):
            continue
        out.append((p[0].replace("/", "_"), p[1], p[2]))
    return out


def patch_survey(root, zone, upc, cycle, phase, apply, backup_root):
    folder = f"{root}/Zone={zone}/Phase={phase}/Cycle={cycle}/UPC={upc}/dlm_kpi_new"
    base = dict(zone=zone, phase=phase, cycle=cycle, upc=upc)
    files = sorted(e.rstrip("/") for e in list_dir(folder) if e.rstrip("/").endswith(".parquet"))
    if not files:
        return [dict(base, status="NO SILVER FILES", note=folder)]
    rows = []
    for f in files:
        path = f"{folder}/{f}"
        try:
            raw = read_file_bytes(path)
            df = pd.read_parquet(io.BytesIO(raw))
            out, changed = apply_run_key({f: df}, upc, phase, cycle)
            n = changed.get(f, 0)
            if n == 0:
                rows.append(dict(base, table=f[:-8], ids_changed=0, rows=len(df), status="ALREADY OK"))
                continue
            new = out[f]
            if len(new) != len(df) or list(new.columns) != list(df.columns):
                rows.append(dict(base, table=f[:-8], ids_changed=n, rows=len(df), status="ERROR",
                                 note="row / column mismatch after rewrite - not written"))
                continue
            if apply:
                write_file_bytes(f"{backup_root}/Zone={zone}/Phase={phase}/Cycle={cycle}/UPC={upc}/dlm_kpi_new/{f}", raw)
                buf = io.BytesIO()
                new.to_parquet(buf, index=False)
                write_file_bytes(path, buf.getvalue())
                back = pd.read_parquet(io.BytesIO(read_file_bytes(path)))        # read back
                key = run_key(upc, phase, cycle)
                bad = 0
                for c in ID_COLUMNS:
                    if c in back.columns:
                        s = back[c].dropna().astype(str)
                        bad += int((s.str.startswith(upc + "_") & ~s.str.startswith(key + "_")).sum())
                if bad or len(back) != len(df):
                    write_file_bytes(path, raw)                                    # restore the original
                    rows.append(dict(base, table=f[:-8], ids_changed=n, rows=len(df), status="ERROR",
                                     note="read-back check failed - original restored"))
                    continue
            rows.append(dict(base, table=f[:-8], ids_changed=n, rows=len(df), status="PATCHED" if apply else "WOULD PATCH"))
        except Exception as e:
            rows.append(dict(base, table=f[:-8], status="ERROR", note=f"{type(e).__name__}: {e}"))
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-file", required=True)
    ap.add_argument("--zone", default="B")
    ap.add_argument("--silver-root", default=os.environ.get("SILVER_S3_ROOT") or DEFAULT_SILVER_ROOT)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)

    root = a.silver_root.rstrip("/")
    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup_root = f"{root}/_idkey_backup/{stamp}"
    surveys = parse_surveys(a.input_file)
    print(f"{'APPLY' if a.apply else 'DRY RUN'}  {len(surveys)} survey(s)  zone {a.zone}  silver {root}", flush=True)
    report = []
    for n, (upc, cycle, phase) in enumerate(surveys, 1):
        rows = patch_survey(root, a.zone, upc, cycle, phase, a.apply, backup_root)
        report += rows
        chg = sum(int(r.get("ids_changed") or 0) for r in rows)
        err = [r for r in rows if r["status"] in ("ERROR", "NO SILVER FILES")]
        print(f"[{n}/{len(surveys)}] {upc} C{cycle} P{phase}: {len(rows)} tables, ids {'changed' if a.apply else 'to change'} "
              f"{chg:,}, problems {len(err)}" + (f"  -> {err[0]['status']} {err[0].get('note', '')}" if err else ""), flush=True)

    buf = io.StringIO()
    w = csv.DictWriter(buf, COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(report)
    data = buf.getvalue().encode("utf-8")
    name = f"run_key_{stamp}{'' if a.apply else '_dry'}.csv"
    os.makedirs(os.path.join(ROOT, "patch", "logs"), exist_ok=True)
    with open(os.path.join(ROOT, "patch", "logs", name), "wb") as fh:
        fh.write(data)
    write_file_bytes(f"{root}/_patch_logs/{name}", data)
    st = pd.DataFrame(report)["status"].value_counts().to_dict() if report else {}
    print(f"\nstatus: {st}\nreport: patch/logs/{name}  and  {root}/_patch_logs/{name}")
    if a.apply:
        print(f"originals kept in {backup_root}/")
    else:
        print("dry run - nothing written. Add --apply to rewrite the IDs.")
    return 1 if any(r["status"] in ("ERROR", "NO SILVER FILES") for r in report) else 0


if __name__ == "__main__":
    sys.exit(main())
