"""
Postgres (dhms_uat) connection settings for the Silver -> DB push (push/).

Same database, user and schema as silver_to_uat_push_1.py. The password is NOT stored in this repo: it comes from the
DB_PASSWORD environment variable or from the git-ignored file config/db.local.env (one line: DB_PASSWORD=...).
Create that file from the push script with:   python3 push/make_db_local_env.py <path of silver_to_uat_push_1.py>
Every value can be overridden by an environment variable of the same name.
"""
import os
from pathlib import Path

_LOCAL = Path(__file__).resolve().parent / "db.local.env"


def _local_env() -> dict:
    out = {}
    if _LOCAL.exists():
        for line in _LOCAL.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out


_L = _local_env()
DB_HOST = os.environ.get("DB_HOST", _L.get("DB_HOST", "dhms-development-dhms-development-postgresql.cdk480kqufch.ap-south-1.rds.amazonaws.com"))
DB_PORT = int(os.environ.get("DB_PORT", _L.get("DB_PORT", "5432")))
DB_NAME = os.environ.get("DB_NAME", _L.get("DB_NAME", "dhms_uat"))
DB_USER = os.environ.get("DB_USER", _L.get("DB_USER", "dhms_admin"))
DB_PASSWORD = os.environ.get("DB_PASSWORD", _L.get("DB_PASSWORD", ""))
DB_SCHEMA = os.environ.get("DB_SCHEMA", _L.get("DB_SCHEMA", "public"))
