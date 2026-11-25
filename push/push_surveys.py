#!/usr/bin/env python3
"""
Push a list of surveys to Postgres, one after the other, and keep the log of every push as a report in S3.

For each survey (UPC CYCLE PHASE, all in the given zone) it runs push/silver_to_uat_push.py with --populate-ids
(project / piu / zone / state / region / contractor / ae_ie IDs are filled from Postgres first; the survey is not pushed when
one cannot be populated). Every survey is its own atomic transaction: a failed survey leaves the DB untouched and the next
survey still runs.

Reports (also when a survey fails), in S3 and in push/logs/<time>/:
    <log-root>/<time>/<UPC>_C<cycle>_P<phase>.log     the full console output of that push
    <log-root>/<time>/push_summary.csv                one row per survey: status, seconds, rows inserted / updated, error
    <log-root>/<time>/push_tables.csv                 one row per survey and table: rows inserted / updated
<log-root> defaults to <silver root>/_push_logs.

    python3 push/push_surveys.py --input-file surveys.txt --zone B                    # dry run (default): nothing is written
    python3 push/push_surveys.py --input-file surveys.txt --zone B --dry-run false    # live push
surveys.txt: lines "UPC CYCLE PHASE" (header, tabs, # comments allowed).
"""
import argparse
import csv
import io
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_S3_ROOT = "s3://dhms-new-stgae/DHMS/NSV/Silver"

SUMMARY_COLS = ["started", "upc", "cycle", "phase", "zone", "mode", "status", "return_code", "seconds",
                "rows_inserted", "rows_updated", "tables", "error", "log_file"]
TABLE_COLS = ["upc", "cycle", "phase", "mode", "table", "inserted", "updated"]
# dry run:  "  master_x : would INSERT 1,000 rows, UPDATE 20 rows (...)"      live:  "  master_x : UPDATED 20 rows, INSERTED 1,000 rows"
RE_DRY = re.compile(r"^\s+(\S+)\s*: would INSERT ([\d,]+) rows, UPDATE ([\d,]+) rows", re.M)
RE_LIVE = re.compile(r"^\s+(\S+)\s*: UPDATED ([\d,]+) rows, INSERTED ([\d,]+) rows", re.M)


def parse_surveys(path):
    out = []
    with open(path, encoding="utf-8-sig") as fh:
        for line in fh:
            parts = re.split(r"[\s,]+", line.strip())
            if len(parts) < 3 or parts[0].lower() == "upc" or parts[0].startswith("#"):
                continue
            out.append((parts[0], parts[1], parts[2]))
    return out


def table_counts(text, live):
    rows = []
    if live:
        for t, u, i in RE_LIVE.findall(text):
            rows.append((t, int(i.replace(",", "")), int(u.replace(",", ""))))
    else:
        for t, i, u in RE_DRY.findall(text):
            rows.append((t, int(i.replace(",", "")), int(u.replace(",", ""))))
    return rows


def put(path, data, s3):
    """Write bytes to S3 (s3://...) or a local path."""
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        s3.put_object(Bucket=bucket, Key=key, Body=data)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)


def to_csv(cols, rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input-file", required=True)
    ap.add_argument("--zone", default="B", help="zone of every survey in the list (default B)")
    ap.add_argument("--dry-run", choices=["true", "false"], default="true")
    ap.add_argument("--no-populate-ids", action="store_true", help="push without filling the IDs from Postgres first")
    ap.add_argument("--s3-root", default=os.environ.get("SILVER_S3_ROOT") or DEFAULT_S3_ROOT)
    ap.add_argument("--log-root", default=None, help="where the reports go (default <s3-root>/_push_logs)")
    ap.add_argument("--push-script", default=str(HERE / "silver_to_uat_push.py"))
    ap.add_argument("--stop-on-failure", action="store_true")
    a = ap.parse_args(argv)

    live = a.dry_run == "false"
    mode = "LIVE" if live else "DRY RUN"
    stamp = time.strftime("%Y%m%d_%H%M%S")
    s3_root = a.s3_root.rstrip("/")
    log_root = f"{(a.log_root or s3_root + '/_push_logs').rstrip('/')}/{stamp}"
    local_dir = HERE / "logs" / f"push_{stamp}"
    surveys = parse_surveys(a.input_file)
    s3 = None
    if log_root.startswith("s3://"):
        import boto3
        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-south-1"))
    print(f"{mode}  {len(surveys)} survey(s)  zone {a.zone}  silver {s3_root}\nreports: {log_root}/   (local copy: {local_dir})", flush=True)

    summary, tables = [], []
    for n, (upc, cycle, phase) in enumerate(surveys, 1):
        name = f"{upc.replace('/', '_')}_C{cycle}_P{phase}.log"
        cmd = [sys.executable, a.push_script, "--upc", upc, "--cycle", cycle, "--phase", phase, "--zone", a.zone,
               "--s3-root", s3_root, "--dry-run", a.dry_run]
        if not a.no_populate_ids:
            cmd.append("--populate-ids")
        t0 = time.time()
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
            text, rc = p.stdout + (("\n--- stderr ---\n" + p.stderr) if p.stderr.strip() else ""), p.returncode
        except Exception as e:                                       # could not even start
            text, rc = f"could not run the push: {type(e).__name__}: {e}", 99
        secs = int(time.time() - t0)
        counts = table_counts(text, live)
        error = ""
        if rc != 0:
            lines = [ln for ln in text.splitlines() if "!!" in ln or "Exception" in ln or "Error" in ln or "aborted" in ln.lower()]
            error = (lines[-1] if lines else text.strip().splitlines()[-1] if text.strip() else "no output")[:300]
        status = ("SUCCESS" if rc == 0 else "FAILED") if live else ("DRY RUN OK" if rc == 0 else "DRY RUN FAILED")
        header = (f"# {mode}  {upc} cycle {cycle} phase {phase} zone {a.zone}  started {started}  "
                  f"status {status}  return code {rc}  {secs}s\n# command: {' '.join(cmd)}\n\n")
        body = (header + text).encode("utf-8")
        put(f"{log_root}/{name}", body, s3)
        put(str(local_dir / name), body, None)
        summary.append(dict(started=started, upc=upc, cycle=cycle, phase=phase, zone=a.zone, mode=mode, status=status,
                            return_code=rc, seconds=secs, rows_inserted=sum(c[1] for c in counts),
                            rows_updated=sum(c[2] for c in counts), tables=len(counts), error=error, log_file=name))
        tables += [dict(upc=upc, cycle=cycle, phase=phase, mode=mode, table=t, inserted=i, updated=u) for t, i, u in counts]
        s = summary[-1]
        print(f"[{n}/{len(surveys)}] {upc} C{cycle} P{phase}: {status}  inserted {s['rows_inserted']:,}  updated "
              f"{s['rows_updated']:,}  {secs}s" + (f"  -> {error}" if error else ""), flush=True)
        # reports are rewritten after every survey, so a stopped run still leaves a usable report
        put(f"{log_root}/push_summary.csv", to_csv(SUMMARY_COLS, summary), s3)
        put(f"{log_root}/push_tables.csv", to_csv(TABLE_COLS, tables), s3)
        put(str(local_dir / "push_summary.csv"), to_csv(SUMMARY_COLS, summary), None)
        put(str(local_dir / "push_tables.csv"), to_csv(TABLE_COLS, tables), None)
        if rc != 0 and a.stop_on_failure:
            print("stopped (--stop-on-failure)")
            break

    bad = [s for s in summary if s["return_code"] != 0]
    print(f"\n{len(summary) - len(bad)} of {len(summary)} survey(s) OK, {len(bad)} failed" +
          ("" if live else "  (dry run: nothing was written; add --dry-run false to push)"))
    for s in bad:
        print(f"  FAILED {s['upc']} C{s['cycle']} P{s['phase']}: {s['error']}")
    print(f"reports: {log_root}/push_summary.csv , push_tables.csv and one .log per survey")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
