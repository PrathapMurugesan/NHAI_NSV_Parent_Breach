"""
Unified filesystem and S3 I/O utilities for the DLM Extraction Pipeline.

Provides transparent abstraction over both local POSIX paths and S3 URIs (s3://...):
  - path_exists(path)
  - list_dir(path)
  - is_dir(path)
  - read_parquet_df(path)
  - read_excel_df(path, sheet_name, header)
  - write_parquet_df(df, path)
  - write_file_bytes(path, content)
  - walk_files(path, extensions)
"""

import io
import os
import re
from typing import Any, List, Optional, Tuple, Dict
import pandas as pd
import pyarrow.parquet as pq

from src.common.logger import get_logger

logger = get_logger(__name__)

_S3_CLIENT = None


def _get_s3_client():
    """Get or initialize cached boto3 S3 client."""
    global _S3_CLIENT
    if _S3_CLIENT is None:
        try:
            import boto3
            region = os.getenv("AWS_REGION", "ap-south-1")
            _S3_CLIENT = boto3.client("s3", region_name=region)
        except Exception as e:
            logger.debug(f"boto3 initialization notice: {e}")
            _S3_CLIENT = False
    return _S3_CLIENT if _S3_CLIENT is not False else None


def _parse_s3_url(url: str) -> Tuple[str, str]:
    """Parse 's3://bucket/key' into (bucket, key)."""
    assert url.startswith("s3://")
    without_scheme = url[5:]
    if "/" in without_scheme:
        bucket, key = without_scheme.split("/", 1)
    else:
        bucket, key = without_scheme, ""
    return bucket, key


def path_exists(path: str) -> bool:
    """Check if a file or directory exists (supports local paths and s3:// URIs)."""
    if not path:
        return False
    if path.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(path)
        if s3:
            if not key:
                try:
                    s3.head_bucket(Bucket=bucket)
                    return True
                except Exception:
                    return False
            # Check exact object
            try:
                s3.head_object(Bucket=bucket, Key=key)
                return True
            except Exception:
                pass
            # Check virtual prefix (directory)
            try:
                prefix = key if key.endswith("/") else key + "/"
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                if resp.get("KeyCount", 0) > 0 or len(resp.get("CommonPrefixes", [])) > 0:
                    return True
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                return resp.get("KeyCount", 0) > 0
            except Exception as e:
                logger.debug(f"Error checking S3 path existence {path}: {e}")
                return False
        else:
            try:
                import pyarrow.fs as pafs
                region = os.getenv("AWS_REGION", "ap-south-1")
                fs = pafs.S3FileSystem(region=region)
                info = fs.get_file_info(f"{bucket}/{key}")
                if info.type != pafs.FileType.NotFound:
                    return True
                sel = pafs.FileSelector(f"{bucket}/{key}", allow_not_found=True, recursive=True)
                return len(fs.get_file_info(sel)) > 0
            except Exception:
                return False
    return os.path.exists(path)


def is_dir(path: str) -> bool:
    """Check if path is a directory / virtual prefix."""
    if not path:
        return False
    if path.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(path)
        if s3:
            if not key:
                return True
            prefix = key if key.endswith("/") else key + "/"
            try:
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
                if resp.get("KeyCount", 0) > 0 or len(resp.get("CommonPrefixes", [])) > 0:
                    return True
                # If exact file object exists and key doesn't end with '/', it's not a dir
                try:
                    s3.head_object(Bucket=bucket, Key=key)
                    return False
                except Exception:
                    pass
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                return resp.get("KeyCount", 0) > 0
            except Exception:
                return False
        else:
            try:
                import pyarrow.fs as pafs
                region = os.getenv("AWS_REGION", "ap-south-1")
                fs = pafs.S3FileSystem(region=region)
                sel = pafs.FileSelector(f"{bucket}/{key}", allow_not_found=True, recursive=False)
                return len(fs.get_file_info(sel)) > 0
            except Exception:
                return False
    return os.path.isdir(path)


def list_dir(path: str) -> List[str]:
    """List entries in a directory / S3 prefix (supports local paths and s3:// URIs)."""
    if not path:
        return []
    if path.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(path)
        if s3:
            prefix = key
            if prefix and not prefix.endswith("/"):
                prefix += "/"
            entries = set()
            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
                    for cp in page.get("CommonPrefixes", []):
                        name = cp.get("Prefix", "")[len(prefix):].rstrip("/")
                        if name:
                            entries.add(name)
                    for obj in page.get("Contents", []):
                        name = obj.get("Key", "")[len(prefix):]
                        if name and "/" not in name:
                            entries.add(name)
                return sorted(entries)
            except Exception as e:
                logger.debug(f"Error listing S3 directory {path}: {e}")
                return []
        else:
            try:
                import pyarrow.fs as pafs
                region = os.getenv("AWS_REGION", "ap-south-1")
                fs = pafs.S3FileSystem(region=region)
                sel = pafs.FileSelector(f"{bucket}/{key}", allow_not_found=True, recursive=True)
                infos = fs.get_file_info(sel)
                base_len = len(f"{bucket}/{key}".rstrip("/")) + 1
                entries = set()
                for info in infos:
                    rel = info.path[base_len:]
                    top = rel.split("/")[0]
                    if top:
                        entries.add(top)
                return sorted(entries)
            except Exception:
                return []
    try:
        return os.listdir(path) if os.path.exists(path) else []
    except Exception:
        return []


def read_parquet_df(path: str) -> pd.DataFrame:
    """Read a Parquet file into pandas DataFrame (supports local paths and s3:// URIs)."""
    if path.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(path)
        if s3:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_parquet(io.BytesIO(obj["Body"].read()))
        else:
            import pyarrow.fs as pafs
            region = os.getenv("AWS_REGION", "ap-south-1")
            fs = pafs.S3FileSystem(region=region)
            table = pq.read_table(f"{bucket}/{key}", filesystem=fs)
            return table.to_pandas()
    return pd.read_parquet(path)


def read_excel_df(path: str, sheet_name: Any = 0, header: Any = 0) -> pd.DataFrame:
    """Read an Excel file into pandas DataFrame (supports local paths and s3:// URIs)."""
    if path.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(path)
        if s3:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_excel(io.BytesIO(obj["Body"].read()), sheet_name=sheet_name, header=header)
        else:
            import pyarrow.fs as pafs
            region = os.getenv("AWS_REGION", "ap-south-1")
            fs = pafs.S3FileSystem(region=region)
            with fs.open_input_stream(f"{bucket}/{key}") as stream:
                return pd.read_excel(io.BytesIO(stream.read()), sheet_name=sheet_name, header=header)
    return pd.read_excel(path, sheet_name=sheet_name, header=header)


def read_file_bytes(path: str) -> bytes:
    """Read a file's raw bytes (local path or s3:// URI). S3 keys are passed as plain strings, so spaces,
    '&' and a double '//' in the key work (pyarrow's URI parser rejects them)."""
    if path.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(path)
        if s3:
            return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        import pyarrow.fs as pafs
        fs = pafs.S3FileSystem(region=os.getenv("AWS_REGION", "ap-south-1"))
        with fs.open_input_stream(f"{bucket}/{key}") as stream:
            return stream.read()
    with open(path, "rb") as fh:
        return fh.read()


def write_parquet_df(df: pd.DataFrame, path: str):
    """Write DataFrame as Parquet to local or S3 destination."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    content = buf.getvalue()
    write_file_bytes(path, content)


def write_file_bytes(path: str, content: bytes):
    """Write raw bytes to local or S3 destination."""
    if path.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(path)
        if s3:
            s3.put_object(Bucket=bucket, Key=key, Body=content)
        else:
            import pyarrow.fs as pafs
            region = os.getenv("AWS_REGION", "ap-south-1")
            fs = pafs.S3FileSystem(region=region)
            with fs.open_output_stream(f"{bucket}/{key}") as stream:
                stream.write(content)
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)


def walk_files(root: str, extensions: Optional[List[str]] = None) -> List[str]:
    """
    Recursively find all files under root matching optional extensions.
    Returns full paths (local or s3://).
    """
    if not root:
        return []
    exts = [e.lower() for e in extensions] if extensions else None

    if root.startswith("s3://"):
        s3 = _get_s3_client()
        bucket, key = _parse_s3_url(root)
        prefix = key
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        matched = []
        if s3:
            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        k = obj.get("Key", "")
                        full_p = f"s3://{bucket}/{k}"
                        if exts is None or any(full_p.lower().endswith(e) for e in exts):
                            matched.append(full_p)
                return matched
            except Exception as e:
                logger.warning(f"Error walking S3 root {root}: {e}")
                return []
        else:
            try:
                import pyarrow.fs as pafs
                region = os.getenv("AWS_REGION", "ap-south-1")
                fs = pafs.S3FileSystem(region=region)
                sel = pafs.FileSelector(f"{bucket}/{key}", allow_not_found=True, recursive=True)
                for info in fs.get_file_info(sel):
                    if info.type == pafs.FileType.File:
                        full_p = f"s3://{info.path}"
                        if exts is None or any(full_p.lower().endswith(e) for e in exts):
                            matched.append(full_p)
                return matched
            except Exception:
                return []

    if not os.path.exists(root):
        return []

    matched = []
    for r, _, files in os.walk(root):
        for f in files:
            full_p = os.path.join(r, f)
            if exts is None or any(full_p.lower().endswith(e) for e in exts):
                matched.append(full_p)
    return matched
