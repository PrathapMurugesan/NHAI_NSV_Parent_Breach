#!/usr/bin/env python3
"""
Check every tracker survey against the Silver bucket the pipeline writes.

A survey is one UPC + zone + phase + cycle. The path is the same one
config/settings.py builds:

    {SILVER_S3_ROOT}/Zone=<zone>/Phase=<phase>/Cycle=<cycle>/UPC=<upc>/
        dlm_kpi_new/<table>.parquet
        dlm_logs/run_summary.csv

Stored means the Silver parquets are there. The run summary says whether the
latest run finished SUCCESS or FAILED. Surveys processed before run logs
existed can be stored with no summary.

Usage (from the repo root, on the EC2 that can read the bucket):

    source .venv/bin/activate
    set -a; source .env; set +a
    python3 scripts/check_silver_s3.py
"""

import argparse
import csv
import io
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.settings import parse_env_file  # noqa: E402

NOT_FOUND = {"404", "NoSuchKey", "NotFound"}
STORED_STATUSES = {"SUCCESS", "STORED_NO_LOG"}


def load_settings(env_file: str) -> dict:
    file_env = parse_env_file(env_file) if os.path.exists(env_file) else {}
    silver = os.environ.get("SILVER_S3_ROOT") or file_env.get("SILVER_S3_ROOT") or "s3://dhms-new-stgae/DHMS/NSV/Silver"
    region = os.environ.get("AWS_REGION") or file_env.get("AWS_REGION") or "ap-south-1"
    return {"silver": silver.rstrip("/"), "region": region}


def split_s3(url: str):
    rest = url[5:]
    bucket, _, key = rest.partition("/")
    return bucket, key.strip("/")


def load_tracker(path: Path):
    jobs = []
    seen = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 5:
            raise SystemExit(f"Bad tracker line (need SNO UPC ZONE PHASE CYCLE): {line}")
        sno, upc, zone, phase, cycle = parts[:5]
        key = (upc, zone, phase, cycle)
        if key in seen:
            continue
        seen.add(key)
        jobs.append({"sno": int(sno), "upc": upc, "zone": zone, "phase": phase, "cycle": cycle})
    jobs.sort(key=lambda row: row["sno"])
    return jobs


def is_missing(exc: ClientError) -> bool:
    code = exc.response.get("Error", {}).get("Code", "")
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in NOT_FOUND or status == 404


class SilverCheck:
    def __init__(self, bucket: str, prefix: str, client):
        self.bucket = bucket
        self.prefix = prefix
        self.client = client

    def upc_prefix(self, zone, phase, cycle, upc) -> str:
        base = f"Zone={zone}/Phase={phase}/Cycle={cycle}/UPC={upc}"
        return f"{self.prefix}/{base}" if self.prefix else base

    def read_summary(self, zone, phase, cycle, upc):
        key = f"{self.upc_prefix(zone, phase, cycle, upc)}/dlm_logs/run_summary.csv"
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if is_missing(exc):
                return None
            raise
        rows = list(csv.DictReader(io.StringIO(body.decode("utf-8"))))
        return rows[-1] if rows else {}

    def parquet_count(self, zone, phase, cycle, upc) -> int:
        prefix = f"{self.upc_prefix(zone, phase, cycle, upc)}/dlm_kpi_new/"
        count = 0
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            page = self.client.list_objects_v2(**kwargs)
            for item in page.get("Contents", []):
                if item["Key"].endswith(".parquet"):
                    count += 1
            if not page.get("IsTruncated"):
                return count
            token = page.get("NextContinuationToken")

    def inspect(self, zone, phase, cycle, upc):
        summary = self.read_summary(zone, phase, cycle, upc)
        tables = self.parquet_count(zone, phase, cycle, upc)
        status = (summary or {}).get("status") or ""
        if status == "SUCCESS" and tables:
            kind = "SUCCESS"
        elif status == "SUCCESS" and not tables:
            kind = "SUCCESS_NO_TABLES"
        elif status == "FAILED":
            kind = "FAILED"
        elif tables:
            kind = "STORED_NO_LOG"
        elif summary:
            kind = status or "UNKNOWN"
        else:
            kind = "NOT_IN_S3"
        return {
            "kind": kind,
            "summary_status": status,
            "parquet_files": tables,
            "dq_pass": (summary or {}).get("dq_pass", ""),
            "dq_warn": (summary or {}).get("dq_warn", ""),
            "dq_fail": (summary or {}).get("dq_fail", ""),
            "finished": (summary or {}).get("finished", ""),
            "error": ((summary or {}).get("error") or "").replace("\n", " ")[:240],
            "checked_cycle": cycle,
        }


def check_job(checker: SilverCheck, job: dict) -> dict:
    zone, phase, cycle, upc = job["zone"], job["phase"], job["cycle"], job["upc"]
    note = ""
    try:
        found = checker.inspect(zone, phase, cycle, upc)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        found = {"kind": "ERROR", "summary_status": "", "parquet_files": 0, "dq_pass": "",
                 "dq_warn": "", "dq_fail": "", "finished": "", "error": code, "checked_cycle": cycle}
    if found["kind"] in {"NOT_IN_S3", "ERROR"} and cycle == "st":
        try:
            alt = checker.inspect(zone, phase, "1", upc)
        except ClientError:
            alt = None
        if alt and alt["kind"] in STORED_STATUSES | {"SUCCESS_NO_TABLES", "FAILED"}:
            note = "sheet cycle st, S3 cycle 1"
            found = alt
    row = dict(job)
    row.update(found)
    row["note"] = note
    return row


def dq_failed(row) -> bool:
    return row["kind"] == "SUCCESS" and str(row.get("dq_fail") or "0") not in {"", "0"}


def write_reports(rows, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    detail = out_dir / f"silver_check_{stamp}.csv"
    remaining = out_dir / f"silver_remaining_{stamp}.txt"
    fields = ["sno", "upc", "zone", "phase", "cycle", "kind", "summary_status", "parquet_files",
              "dq_pass", "dq_warn", "dq_fail", "finished", "error", "note", "checked_cycle"]
    with detail.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    left = [row for row in rows if row["kind"] not in STORED_STATUSES]
    lines = ["# surveys not stored. columns: UPC ZONE PHASE CYCLE"]
    for row in left:
        cycle = row["checked_cycle"] if row["checked_cycle"] != row["cycle"] else row["cycle"]
        if cycle == "st":
            cycle = "1"
        lines.append(f"{row['upc']} {row['zone']} {row['phase']} {cycle}")
    remaining.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return detail, remaining


def print_summary(rows, silver: str):
    total = len(rows)
    counts = {}
    for row in rows:
        counts[row["kind"]] = counts.get(row["kind"], 0) + 1

    def n(kind):
        return counts.get(kind, 0)

    stored = n("SUCCESS") + n("STORED_NO_LOG")
    remaining = total - stored
    accounted = sum(counts.values())
    by_upc = {}
    for row in rows:
        by_upc.setdefault(row["upc"], []).append(row)
    full = part = none = 0
    for group in by_upc.values():
        ok = sum(item["kind"] in STORED_STATUSES for item in group)
        if ok == len(group):
            full += 1
        elif ok == 0:
            none += 1
        else:
            part += 1

    print(flush=True)
    print("=" * 64, flush=True)
    print(f"SILVER ROOT                  {silver}", flush=True)
    print(f"TRACKER SURVEYS              {total}", flush=True)
    print(f"  STORED                     {stored}", flush=True)
    print(f"    SUCCESS WITH TABLES      {n('SUCCESS')}", flush=True)
    print(f"    TABLES, NO RUN LOG       {n('STORED_NO_LOG')}", flush=True)
    print(f"  NOT STORED                 {remaining}", flush=True)
    print(f"    FAILED                   {n('FAILED')}", flush=True)
    print(f"    NOT IN S3                {n('NOT_IN_S3')}", flush=True)
    print(f"    SUCCESS BUT NO TABLES    {n('SUCCESS_NO_TABLES')}", flush=True)
    print(f"    CHECK ERROR              {n('ERROR')}", flush=True)
    other = accounted - stored - n("FAILED") - n("NOT_IN_S3") - n("SUCCESS_NO_TABLES") - n("ERROR")
    if other:
        print(f"    OTHER                    {other}", flush=True)
    print(f"  SUCCESS BUT DQ FAIL        {sum(1 for row in rows if dq_failed(row))}", flush=True)
    print(f"  COUNTED                    {accounted}  (must equal {total})", flush=True)
    print(flush=True)
    print(f"UNIQUE UPCS                  {len(by_upc)}", flush=True)
    print(f"  FULLY STORED               {full}", flush=True)
    print(f"  PARTLY STORED              {part}", flush=True)
    print(f"  NOTHING STORED             {none}", flush=True)
    print("=" * 64, flush=True)
    print(flush=True)
    print("NOT STORED", flush=True)
    for row in rows:
        if row["kind"] in STORED_STATUSES:
            continue
        extra = f"  {row['note']}" if row["note"] else ""
        err = f"  {row['error']}" if row["error"] else ""
        print(
            f"sno {row['sno']:<4} {row['kind']:<20} {row['upc']} {row['zone']} {row['phase']} {row['cycle']}"
            f"  tables={row['parquet_files']}{extra}{err}",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description="Check tracker surveys in the Silver S3 bucket.")
    parser.add_argument("--tracker", default="batches/tracker_surveys.txt", help="SNO UPC ZONE PHASE CYCLE list")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--silver-root", default="", help="Override SILVER_S3_ROOT")
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--out-dir", default="logs")
    parser.add_argument("--quiet", action="store_true", help="Print only the summary, not each survey")
    args = parser.parse_args()

    os.chdir(ROOT)
    settings = load_settings(args.env_file)
    if args.silver_root:
        settings["silver"] = args.silver_root.rstrip("/")
    tracker = Path(args.tracker)
    if not tracker.is_file():
        raise SystemExit(f"Tracker list not found: {tracker}")

    bucket, prefix = split_s3(settings["silver"])
    client = boto3.client(
        "s3",
        region_name=settings["region"],
        config=Config(retries={"max_attempts": 8, "mode": "standard"}),
    )
    checker = SilverCheck(bucket, prefix, client)
    jobs = load_tracker(tracker)
    print(f"Silver root: {settings['silver']}", flush=True)
    print(f"Tracker: {tracker}  surveys: {len(jobs)}", flush=True)
    print(flush=True)

    rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(check_job, checker, job) for job in jobs]
        for done, future in enumerate(as_completed(futures), start=1):
            row = future.result()
            rows.append(row)
            if not args.quiet:
                flag = f"  {row['note']}" if row["note"] else ""
                if dq_failed(row):
                    flag += f"  DQ_FAIL={row['dq_fail']}"
                print(
                    f"[{done}/{len(jobs)}] {row['kind']:<20} sno {row['sno']:<4} "
                    f"{row['upc']} {row['zone']} {row['phase']} {row['cycle']}  tables={row['parquet_files']}{flag}",
                    flush=True,
                )
    rows.sort(key=lambda row: row["sno"])
    print_summary(rows, settings["silver"])
    detail, remaining = write_reports(rows, Path(args.out_dir))
    print(flush=True)
    print(f"Detail CSV:     {detail}", flush=True)
    print(f"Not stored list:{remaining}", flush=True)


if __name__ == "__main__":
    main()
