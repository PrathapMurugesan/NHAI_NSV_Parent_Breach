#!/usr/bin/env python3
"""
Remove the rows with the OLD record IDs (without _P<phase>_C<cycle>) of the given surveys from Postgres.

After the Silver tables got the new IDs (patch/patch_run_key.py or a re-extraction) and were pushed, the DB holds both the new
rows and the old ones (the push matches on `id`). This deletes only the old ones:
    id LIKE '<UPC>\\_%'  AND  id NOT LIKE '<UPC>\\_P<phase>\\_C<cycle>\\_%'      (the UPC's rows, upc column match)
Run it AFTER the push of the new IDs. Dry run by default (counts only). --apply deletes, all surveys in ONE transaction, children
before parents; any error rolls everything back. The rows are first saved to push/logs/old_id_rows_<time>/<table>.csv.

    python3 push/delete_old_id_rows.py --input-file surveys.txt --zone B            # counts
    python3 push/delete_old_id_rows.py --input-file surveys.txt --zone B --apply    # delete
surveys.txt: lines "UPC CYCLE PHASE". Uses the same DB settings as the push (.env of the pipeline / environment).
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))


def esc(s):
    return s.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-file", required=True)
    ap.add_argument("--zone", default="B")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    import psycopg2
    import silver_to_uat_push as push
    push.load_env()
    s = push.db_settings()
    miss = [k for k in ("host", "dbname", "user", "password") if not s.get(k)]
    if miss:
        sys.exit(f"DB settings not found for {miss}")
    surveys = []
    for line in open(a.input_file, encoding="utf-8-sig"):
        p = re.split(r"[\s,]+", line.strip())
        if len(p) >= 3 and p[0].lower() != "upc" and not p[0].startswith("#"):
            surveys.append((p[0], p[1], p[2]))
    print(f"{'APPLY' if a.apply else 'DRY RUN'}  {s['user']}@{s['host']}/{s['dbname']}  {len(surveys)} survey(s)", flush=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    save = HERE / "logs" / f"old_id_rows_{stamp}"
    conn = psycopg2.connect(connect_timeout=15, application_name="silver_old_id_cleanup", **s)
    conn.autocommit = False
    total = 0
    try:
        with conn.cursor() as cur:
            tables = []
            for _pq, primary, fallbacks in push.SILVER_TABLE_SPECS:
                t = next((x for x in [primary, *fallbacks] if push.db_table_exists(cur, push.DB_SCHEMA, x)), None)
                if t and {"id", "upc"} <= set(push.db_columns(cur, push.DB_SCHEMA, t)) and t not in tables:
                    tables.append(t)
            for upc, cycle, phase in surveys:
                u = upc.replace("/", "_")
                old = f"{esc(u)}\\_%"
                new = f"{esc(u)}\\_P{phase}\\_C{cycle}\\_%"
                variants = push.parse_upc_variations(upc)
                # children before parents: tables listed last in the push order are deleted first
                for t in reversed(tables):
                    where = f"upc = ANY(%s) AND id LIKE %s AND id NOT LIKE %s"
                    cur.execute(f"SELECT count(*) FROM {push.DB_SCHEMA}.{t} WHERE {where}", (variants, old, new))
                    n = cur.fetchone()[0]
                    cur.execute(f"SELECT count(*) FROM {push.DB_SCHEMA}.{t} WHERE upc = ANY(%s) AND id LIKE %s", (variants, new))
                    n_new = cur.fetchone()[0]
                    if n:
                        print(f"  {u} C{cycle} P{phase} {t}: old-ID rows {n:,}  (new-ID rows present {n_new:,})")
                    if n and a.apply:
                        if not n_new:
                            raise RuntimeError(f"{t}: {n} old rows but NO new-ID rows for {u} - push the new IDs first")
                        os.makedirs(save, exist_ok=True)
                        with open(save / f"{t}.csv", "a", encoding="utf-8") as fh:
                            sel = cur.mogrify(f"SELECT * FROM {push.DB_SCHEMA}.{t} WHERE {where}", (variants, old, new)).decode()
                            cur.copy_expert(f"COPY ({sel}) TO STDOUT WITH CSV HEADER", fh)
                        cur.execute(f"DELETE FROM {push.DB_SCHEMA}.{t} WHERE {where}", (variants, old, new))
                        total += cur.rowcount
                    elif n:
                        total += n
        if a.apply:
            conn.commit()
            print(f"\ncommitted: {total:,} old-ID rows deleted (copy in {save})")
        else:
            conn.rollback()
            print(f"\ndry run: {total:,} old-ID rows would be deleted. Add --apply.")
    except Exception as e:
        conn.rollback()
        print(f"\nFAILED, nothing changed: {type(e).__name__}: {e}")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
