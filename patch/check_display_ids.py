#!/usr/bin/env python3
"""
Read-only check: are the display_defect_id populated in the Silver tables of the given UPCs?

For every table of the UPC that has breach_flag, it counts the breach rows, those with / without a display_defect_id,
non-breach rows that carry an ID, and duplicate top-level IDs inside the table. Status is OK when no breach row
is missing its ID. Nothing is written to S3 (only patch/logs/check_ids_<time>.csv locally).

    python3 patch/check_display_ids.py --list upcs.txt          # lines: UPC  CYCLE  PHASE  (tab / space / comma; header ok)
    python3 patch/check_display_ids.py --upc N/01002/01001/GJ --cycle 1 --phase 1
    python3 patch/check_display_ids.py --all                    # every UPC under the silver root
"""
import argparse
import csv
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "patch"))

import pandas as pd

import patch_parent_breach as P

TOP_RE = re.compile(r"^D/\d{6}/N/\d{5}$")


def parse_list(path):
    out = []
    for line in open(path, encoding="utf-8-sig"):
        parts = re.split(r"[\t, ]+", line.strip())
        if len(parts) < 3 or parts[0].lower() == "upc" or parts[0].startswith("#"):
            continue
        out.append((parts[0].replace("/", "_"), parts[1], parts[2]))        # upc, cycle, phase
    return out


def check_upc(root, zone, phase, cycle, upc):
    folder = f"{root}/Zone={zone}/Phase={phase}/Cycle={cycle}/UPC={upc}/dlm_kpi_new"
    rows = []
    for name in sorted(n for n in P.list_dir(folder) if n.endswith(".parquet")):
        t = name[:-8]
        try:
            df = P._read(f"{folder}/{name}", ["breach_flag", "display_defect_id"])
        except Exception:
            continue                                            # table without these columns
        br = pd.to_numeric(df["breach_flag"], errors="coerce").fillna(0).astype(int) == 1
        ids = df["display_defect_id"]
        has = ids.notna() & (ids.astype(str) != "")
        top = ids[has].astype(str)
        top = top[top.map(lambda v: bool(TOP_RE.match(v)))]
        miss = int((br & ~has).sum())
        rows.append(dict(zone=zone, phase=phase, cycle=cycle, upc=upc, table=t, rows=len(df), breach=int(br.sum()),
                         breach_with_id=int((br & has).sum()), breach_missing_id=miss,
                         nonbreach_with_id=int((~br & has).sum()), duplicate_top_ids=int(top.duplicated().sum()),
                         status="OK" if miss == 0 else "MISSING"))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--silver-root", default=os.environ.get("SILVER_S3_ROOT", P.DEFAULT_SILVER_ROOT))
    ap.add_argument("--list")
    ap.add_argument("--upc")
    ap.add_argument("--cycle")
    ap.add_argument("--phase")
    ap.add_argument("--all", action="store_true")
    a = ap.parse_args()
    root = a.silver_root.rstrip("/")

    wanted = parse_list(a.list) if a.list else ([(a.upc.replace("/", "_"), a.cycle, a.phase)] if a.upc else None)
    if wanted is None and not a.all:
        ap.error("give --list, --upc (with --cycle --phase) or --all")
    found = P.discover(root)                                    # [(zone, phase, cycle, upc)]
    todo = found if wanted is None else []
    notfound = []
    for upc, cycle, phase in wanted or []:
        hit = [k for k in found if k[3] == upc and k[2] == str(cycle) and k[1] == str(phase)]
        todo += hit
        if not hit:
            notfound.append((upc, cycle, phase))

    report = []
    for n, (zone, phase, cycle, upc) in enumerate(todo, 1):
        rows = check_upc(root, zone, phase, cycle, upc)
        report += rows
        bad = [r for r in rows if r["status"] != "OK"]
        tot_b = sum(r["breach"] for r in rows)
        tot_m = sum(r["breach_missing_id"] for r in rows)
        print(f"[{n}/{len(todo)}] {upc} Z={zone} P={phase} C={cycle}: {len(rows)} tables, breach rows {tot_b}, "
              f"missing IDs {tot_m} -> {'OK' if not bad else 'MISSING in ' + ', '.join(r['table'] for r in bad)}")
    for upc, cycle, phase in notfound:
        print(f"NOT FOUND under silver root: {upc} cycle {cycle} phase {phase}")

    os.makedirs(os.path.join(ROOT, "patch", "logs"), exist_ok=True)
    out = os.path.join(ROOT, "patch", "logs", f"check_ids_{time.strftime('%Y%m%d_%H%M%S')}.csv")
    cols = ["zone", "phase", "cycle", "upc", "table", "rows", "breach", "breach_with_id", "breach_missing_id",
            "nonbreach_with_id", "duplicate_top_ids", "status"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, cols)
        w.writeheader()
        w.writerows(report)
    miss = [r for r in report if r["status"] != "OK"]
    print(f"\n{len(todo)} UPC-cycle(s) checked, {len(notfound)} not found, {len(miss)} table(s) with breach rows missing an ID")
    print(f"detail: {out}")
    return 1 if miss or notfound else 0


if __name__ == "__main__":
    sys.exit(main())
