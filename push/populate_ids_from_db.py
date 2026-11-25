#!/usr/bin/env python3
"""
Check / populate the project-level IDs (project_id, piu_id, zone_id, state_id, region_id, contractor_id, ae_ie_id) of the
Silver tables from Postgres, BEFORE the Silver -> DB push, and check them in the DB AFTER the push.

The values come from the UPC's row in master_upc_project (mapped through the DB's foreign keys, see
src/output/db_id_mapping.py). The DB is only read (read-only session). Nothing is written unless --apply.

  1. check (default)  every survey / table / ID column: blank before, what Postgres gives, blank after, status
                      OK | FILLED | REPLACED | UNMAPPED | NO PROJECT      -> push/logs/id_populate_<time>.csv
  2. --apply          write the populated Silver parquets back to S3 (originals kept in <s3-root>/_idpop_backup/<time>/),
                      then run the push with:  python3 push/silver_to_uat_push.py --input-file surveys.txt --dry-run false
  3. --check-db       after the push: count the NULL / blank ID columns of the UPC's rows in the DB tables

    python3 push/populate_ids_from_db.py --input-file surveys.txt               # check
    python3 push/populate_ids_from_db.py --input-file surveys.txt --apply       # populate Silver
    python3 push/populate_ids_from_db.py --input-file surveys.txt --check-db    # after the push
surveys.txt: lines "UPC CYCLE PHASE [ZONE]" (header and # comments allowed).
"""
import argparse
import csv
import io
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

import pandas as pd

from src.output.db_id_mapping import ID_NAMES, PROJECT_TABLE, derive_sources, populate_df

COLS = ["upc", "cycle", "phase", "zone", "table", "column", "rows", "blank_before", "db_value", "source", "how",
        "filled", "replaced", "blank_after", "status", "note"]
OK_STATUS = ("OK", "FILLED", "REPLACED", "NOT IN SILVER", "SKIPPED")
BAD_STATUS = ("UNMAPPED", "NO PROJECT", "NO SILVER FILES", "ERROR", "BLANK IN DB")


def _bytes(path, s3):
    if path.startswith("s3://"):
        b, k = path[5:].split("/", 1)
        return s3.get_object(Bucket=b, Key=k)["Body"].read()
    with open(path, "rb") as fh:
        return fh.read()


def _put(path, data, s3):
    if path.startswith("s3://"):
        b, k = path[5:].split("/", 1)
        s3.put_object(Bucket=b, Key=k, Body=data)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)


def project_row(cur, push, upc, cycle, phase):
    """The UPC's row of master_upc_project as a dict (None when absent / ambiguous) and a note."""
    cur.execute(f"SELECT * FROM {push.DB_SCHEMA}.{PROJECT_TABLE} WHERE upc = ANY(%s)", (push.parse_upc_variations(upc),))
    names = [d[0] for d in cur.description]
    rows = [dict(zip(names, r)) for r in cur.fetchall()]
    for col, val in (("cycle", cycle), ("phase", phase)):
        if len(rows) > 1 and col in names:
            rows = [r for r in rows if str(r[col]) == str(val)] or rows
    if not rows:
        return None, f"no row in {PROJECT_TABLE} for this UPC"
    if len(rows) > 1:
        return None, f"{len(rows)} rows in {PROJECT_TABLE} for this UPC - cannot choose"
    return rows[0], ""


def run_survey(push, cur, s3, upc, cycle, phase, zone, s3_root, apply=False, backup_root=""):
    """Check (and with apply=True populate) one survey. Returns report rows."""
    base = dict(upc=upc, cycle=cycle, phase=phase, zone=zone or "")
    prow, note = project_row(cur, push, upc, cycle, phase)
    if prow is None:
        return [dict(base, status="NO PROJECT", note=note)]
    if not zone:
        zone = push.resolve_zone_from_db(cur, upc) or ""
        base["zone"] = zone
    pmap = push.locate_s3_parquet_files(s3, s3_root, upc, phase, cycle, zone or None)
    if not pmap:
        return [dict(base, status="NO SILVER FILES", note="no parquet found on S3 for this survey")]
    proj_fks = push.foreign_keys(cur, push.DB_SCHEMA, PROJECT_TABLE)
    rows = []
    for pq_file, primary, fallbacks in push.SILVER_TABLE_SPECS:
        src = pmap.get(pq_file)
        if not src:
            rows.append(dict(base, table=pq_file[:-8], status="NO FILE", note="parquet not on S3"))
            continue
        table = next((t for t in [primary, *fallbacks] if push.db_table_exists(cur, push.DB_SCHEMA, t)), None)
        if table is None:
            rows.append(dict(base, table=pq_file[:-8], status="NO DB TABLE", note=f"{primary} not in the DB"))
            continue
        raw = _bytes(src, s3)
        df = pd.read_parquet(io.BytesIO(raw))
        db_cols = push.db_columns(cur, push.DB_SCHEMA, table)
        cols = [c for c in df.columns if c in db_cols]
        sources = derive_sources(prow, push.foreign_keys(cur, push.DB_SCHEMA, table), proj_fks, cols)
        out, stats = populate_df(df, sources)
        for st in stats:
            rows.append(dict(base, table=table, note="", **st))
        absent = [c for c in ID_NAMES if c in db_cols and c not in df.columns]
        if absent:
            rows.append(dict(base, table=table, column=",".join(absent), status="NOT IN SILVER",
                             note="DB column has no Silver column - left to the DB default / NULL"))
        if apply and any(s["filled"] or s["replaced"] for s in stats):
            rel = src.split("/", 3)[3] if src.startswith("s3://") else Path(src).name
            _put(f"{backup_root}/{rel}", raw, s3)                    # original first
            buf = io.BytesIO()
            out.to_parquet(buf, index=False)
            _put(src, buf.getvalue(), s3)
    return rows


def check_db(push, cur, upc, cycle, phase, zone):
    """After the push: NULL / blank ID columns of the UPC's rows in each DB table."""
    base = dict(upc=upc, cycle=cycle, phase=phase, zone=zone or "")
    rows = []
    for pq_file, primary, fallbacks in push.SILVER_TABLE_SPECS:
        table = next((t for t in [primary, *fallbacks] if push.db_table_exists(cur, push.DB_SCHEMA, t)), None)
        if table is None:
            continue
        cols = push.db_columns(cur, push.DB_SCHEMA, table)
        if "upc" not in cols:
            rows.append(dict(base, table=table, status="SKIPPED", note="table has no upc column"))
            continue
        for c in [c for c in ID_NAMES if c in cols]:
            cur.execute(f"SELECT count(*), count(*) FILTER (WHERE {c} IS NULL OR {c}::text = '') "
                        f"FROM {push.DB_SCHEMA}.{table} WHERE upc = ANY(%s)", (push.parse_upc_variations(upc),))
            n, nb = cur.fetchone()
            rows.append(dict(base, table=table, column=c, rows=n, blank_after=nb,
                             status="OK" if n and not nb else ("NO ROWS" if not n else "BLANK IN DB")))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-file", required=True)
    ap.add_argument("--s3-root", default=None)
    ap.add_argument("--apply", action="store_true", help="write the populated Silver parquets (originals backed up)")
    ap.add_argument("--check-db", action="store_true", help="check the ID columns in the DB (after the push)")
    a = ap.parse_args()

    import psycopg2
    import silver_to_uat_push as push                       # same credentials as the push (config/db_config.py)
    push.load_env()
    s3_root = (a.s3_root or os.environ.get("SILVER_S3_ROOT") or push.DEFAULT_S3_ROOT).rstrip("/")
    surveys = []
    with open(a.input_file, encoding="utf-8-sig") as fh:
        for line in fh:
            if line.strip() and not line.lower().startswith(("upc", "#")):
                surveys.append(push.parse_survey_input(line))
    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup_root = f"{s3_root}/_idpop_backup/{stamp}"
    s = push.db_settings()
    mode = "APPLY" if a.apply else ("CHECK DB" if a.check_db else "CHECK")
    print(f"{mode}  {s['user']}@{s['host']}/{s['dbname']}  {len(surveys)} survey(s)  silver {s3_root}")
    from config import db_config
    missing = [k for k in ("host", "dbname", "user", "password") if not s.get(k)]
    if missing:
        sys.exit(f"DB settings not found on this machine for: {missing}. Looked for - {db_config.describe()}. "
                 "Export them (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD or the PG* names) or put them in the pipeline .env.")
    print("DB settings: " + db_config.describe())
    conn = psycopg2.connect(connect_timeout=15, application_name="silver_id_populate", **s)
    conn.set_session(readonly=True, autocommit=True)         # the DB is only read
    s3 = push.get_s3_client()
    report = []
    try:
        with conn.cursor() as cur:
            for n, (upc, cycle, phase, zone) in enumerate(surveys, 1):
                try:
                    if a.check_db:
                        rows = check_db(push, cur, upc, cycle, phase, zone)
                    else:
                        rows = run_survey(push, cur, s3, upc, cycle, phase, zone, s3_root, a.apply, backup_root)
                except Exception as e:
                    rows = [dict(upc=upc, cycle=cycle, phase=phase, zone=zone or "", status="ERROR",
                                 note=f"{type(e).__name__}: {e}")]
                report += rows
                bad = [r for r in rows if r.get("status") not in OK_STATUS]
                chg = sum(int(r.get("filled") or 0) + int(r.get("replaced") or 0) for r in rows)
                what = "; ".join(sorted({f"{r.get('table', '')}.{r.get('column', '')} {r['status']}" for r in bad}))[:300]
                print(f"[{n}/{len(surveys)}] {upc} C{cycle} P{phase}: {len(rows)} checks, values to fill/replace {chg}, "
                      f"problems {len(bad)}" + (f"  -> {what}" if bad else ""))
    finally:
        conn.close()
    os.makedirs(HERE / "logs", exist_ok=True)
    out = HERE / "logs" / f"id_populate_{stamp}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(report)
    st = pd.DataFrame(report)["status"].value_counts().to_dict() if report else {}
    print(f"\nstatus: {st}\nreport: {out}")
    if a.apply:
        print(f"originals backed up under {backup_root}/ - now run the push (push/silver_to_uat_push.py ... --dry-run false)")
    elif not a.check_db:
        print("check only - nothing was written. UNMAPPED / NO PROJECT must be fixed before the push; "
              "--apply populates the Silver tables.")
    return 1 if any(r.get("status") in BAD_STATUS for r in report) else 0


if __name__ == "__main__":
    sys.exit(main())
