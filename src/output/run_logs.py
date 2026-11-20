"""
Per-UPC run log, DQ report and run summary, written next to the Silver tables of the UPC:

    <silver UPC root>/dlm_logs/run_<time>.log        the full log of this run (also kept when the run fails)
    <silver UPC root>/dlm_logs/dq_report.csv         the validation (DQ) checks V1 - V12 of the latest run
    <silver UPC root>/dlm_logs/run_summary.csv       one row per run: status, duration, tables, DQ counts, error

<silver UPC root> is the folder that holds dlm_kpi_new/ (S3 or local), next to image_upload_log.csv.
"""

import csv
import io
import logging
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Optional

from src.common.fs_utils import path_exists, read_file_bytes, write_file_bytes
from src.common.logger import DEFAULT_DATE_FORMAT, DEFAULT_LOG_FORMAT, get_logger

logger = get_logger("dlm_pipeline")

SUMMARY_COLS = ["started", "finished", "seconds", "zone", "phase", "cycle", "upc", "status", "tables", "rows",
                "dq_pass", "dq_warn", "dq_fail", "dq_skip", "error", "log_file"]


def logs_root(silver_output_dir: str) -> str:
    """<UPC root>/dlm_logs for settings.silver_output_dir = <UPC root>/dlm_kpi_new."""
    upc_root = os.path.dirname(silver_output_dir.rstrip("/\\"))
    return f"{upc_root}/dlm_logs"


class RunLog:
    """Context that mirrors the run's log into a file and ships it, with the summary, to the UPC's dlm_logs folder."""

    def __init__(self, settings):
        self.s = settings
        self.stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.started = datetime.now()
        self.root = logs_root(settings.silver_output_dir) if settings.silver_output_dir else ""
        self.local = os.path.join(tempfile.gettempdir(), f"dlm_{settings.upc}_P{settings.phase}_C{settings.cycle}_{self.stamp}.log")
        self.handler: Optional[logging.Handler] = None
        self.result: Dict = {}
        self.error = ""

    def __enter__(self):
        h = logging.FileHandler(self.local, encoding="utf-8")
        h.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT))
        h.setLevel(logging.INFO)
        for name in ("dlm_pipeline", ""):
            logging.getLogger(name).addHandler(h)           # the pipeline logger and the module loggers
        self.handler = h
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is not None:
            import traceback
            self.error = "".join(traceback.format_exception(exc_type, exc, tb))[-4000:]
            logger.error("Run failed for %s:\n%s", self.s.upc, self.error)
        self.finish()
        return False

    def finish(self) -> None:
        for name in ("dlm_pipeline", ""):
            logging.getLogger(name).removeHandler(self.handler)
        self.handler.close()
        if not self.root:
            return
        try:
            with open(self.local, "rb") as fh:
                write_file_bytes(f"{self.root}/run_{self.stamp}.log", fh.read())
            self._append_summary()
            logger.info("Run log and summary written to %s", self.root)
        except Exception as e:                              # logging must never fail the run
            logger.warning("Could not write the run log to %s: %s", self.root, e)

    def _append_summary(self) -> None:
        path = f"{self.root}/run_summary.csv"
        old: List[Dict] = []
        if path_exists(path):
            old = list(csv.DictReader(io.StringIO(read_file_bytes(path).decode("utf-8"))))
        dq = (self.result or {}).get("validation_summary", {}) or {}
        now = datetime.now()
        row = dict(started=self.started.strftime("%Y-%m-%d %H:%M:%S"), finished=now.strftime("%Y-%m-%d %H:%M:%S"),
                   seconds=int((now - self.started).total_seconds()), zone=self.s.zone, phase=self.s.phase,
                   cycle=self.s.cycle, upc=self.s.upc,
                   status=(self.result or {}).get("status", "FAILED" if self.error else "UNKNOWN"),
                   tables=len((self.result or {}).get("tables", [])), rows=(self.result or {}).get("rows", ""),
                   dq_pass=dq.get("PASS", 0), dq_warn=dq.get("WARN", 0), dq_fail=dq.get("FAIL", 0), dq_skip=dq.get("SKIP", 0),
                   error=self.error.strip().splitlines()[-1] if self.error else "", log_file=f"run_{self.stamp}.log")
        buf = io.StringIO()
        w = csv.DictWriter(buf, SUMMARY_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(old + [row])
        write_file_bytes(path, buf.getvalue().encode("utf-8"))
