"""
Postgres (dhms_uat) connection settings for the Silver -> DB push (push/).

Nothing secret is stored in the repo. The settings are the ones already on the machine, looked up in this order:
  1. environment variables
  2. the pipeline's .env (repo root) and config/db.local.env (git-ignored)
under any of the usual names:
  host      DB_HOST      PGHOST          POSTGRES_HOST      DATABASE_HOST
  port      DB_PORT      PGPORT          POSTGRES_PORT
  database  DB_NAME      PGDATABASE      POSTGRES_DB        DB_DATABASE
  user      DB_USER      PGUSER          POSTGRES_USER
  password  DB_PASSWORD  PGPASSWORD      POSTGRES_PASSWORD  DB_PASS
  schema    DB_SCHEMA    (default public)
Values in .env that are shell expressions (${VAR:-x}) are skipped - export those as environment variables.
describe() lists which names were found (never the values).
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parent.parent
_FILES = [_ROOT / ".env", Path(__file__).resolve().parent / "db.local.env"]

_NAMES: Dict[str, List[str]] = {
    "host": ["DB_HOST", "PGHOST", "POSTGRES_HOST", "DATABASE_HOST"],
    "port": ["DB_PORT", "PGPORT", "POSTGRES_PORT"],
    "database": ["DB_NAME", "PGDATABASE", "POSTGRES_DB", "DB_DATABASE"],
    "user": ["DB_USER", "PGUSER", "POSTGRES_USER"],
    "password": ["DB_PASSWORD", "PGPASSWORD", "POSTGRES_PASSWORD", "DB_PASS"],
    "schema": ["DB_SCHEMA"],
}


def _file_values() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for f in _FILES:
        if not f.exists():
            continue
        for line in f.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[7:].strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            if v and "${" not in v and "$(" not in v:
                out[k.strip()] = v
    return out


_FILE = _file_values()


def _find(key: str) -> Tuple[Optional[str], str]:
    """(value, name it was found under / '')"""
    for name in _NAMES[key]:
        if os.environ.get(name):
            return os.environ[name], f"{name} (environment)"
    for name in _NAMES[key]:
        if _FILE.get(name):
            return _FILE[name], f"{name} (.env / db.local.env)"
    return None, ""


def describe() -> str:
    return "; ".join(f"{k}: {_find(k)[1] or 'NOT FOUND'}" for k in _NAMES)


DB_HOST = _find("host")[0] or ""
DB_PORT = int(_find("port")[0] or 5432)
DB_NAME = _find("database")[0] or ""
DB_USER = _find("user")[0] or ""
DB_PASSWORD = _find("password")[0] or ""
DB_SCHEMA = _find("schema")[0] or "public"
