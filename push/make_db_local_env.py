#!/usr/bin/env python3
"""Write config/db.local.env (git-ignored) with the DB_* values of the existing push script - same credentials, unchanged."""
import os
import re
import sys

src = sys.argv[1] if len(sys.argv) > 1 else "silver_to_uat_push_1.py"
text = open(src, encoding="utf-8").read()
out = []
for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SCHEMA"):
    m = re.search(rf"^{key}\s*=\s*[\"']?([^\"'\n]+?)[\"']?\s*(?:#.*)?$", text, re.M)
    if not m:
        sys.exit(f"{key} not found in {src}")
    out.append(f"{key}={m.group(1)}")
dst = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "db.local.env")
open(dst, "w", encoding="utf-8").write("\n".join(out) + "\n")
os.chmod(dst, 0o600)
print(f"written {dst} ({len(out)} values; password not shown)")
