"""
Table schema loader and definition for the DLM Extraction Pipeline.

Defines the exact column specifications and types for all 14 final tables:
  1. master_cracking_10m (89 columns)
  2. master_cracking_as_per_ca (89 columns)
  3. master_pothole (88 columns)
  4. master_ravelling (92 columns)
  5. master_ravelling_10m (92 columns)
  6. master_roughness_as_per_ca (93 columns)
  7. master_roughness_child_10m (93 columns)
  8. master_roughness_parent (93 columns)
  9. master_rutting_as_per_ca (92 columns)
  10. master_rutting_child_10m (92 columns)
  11. master_rutting_parent (92 columns)
  12. geometry_parent (22 columns)
  13. geometry_child (13 columns)
  14. master_chainage (54 columns)
alongside dlm_validation_results.
"""

import os
import json
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from src.common.logger import get_logger

logger = get_logger(__name__)

# Load exact final schemas from final_schemas.json if present
_SCHEMA_JSON_PATH = os.path.join(os.path.dirname(__file__), "final_schemas.json")
if os.path.exists(_SCHEMA_JSON_PATH):
    with open(_SCHEMA_JSON_PATH, "r") as _f:
        FINAL_TABLE_SCHEMAS: Dict[str, List[str]] = json.load(_f)
else:
    FINAL_TABLE_SCHEMAS = {}

# Complete Silver layer tables list
SILVER_TABLES = [
    "master_cracking_10m",
    "master_cracking_as_per_ca",
    "master_pothole",
    "master_ravelling",
    "master_ravelling_10m",
    "master_roughness_as_per_ca",
    "master_roughness_child_10m",
    "master_roughness_parent",
    "master_rutting_as_per_ca",
    "master_rutting_child_10m",
    "master_rutting_parent",
    "master_roughness_1000m_lane",
    "master_rutting_1000m_lane",
    "master_cracking_1000m_lane",
    "master_ravelling_1000m_lane",
    "geometry_parent",
    "geometry_child",
    "master_chainage",
    "dlm_validation_results",
]

SHEET_TO_TABLE = {
    "pothole": "master_pothole",
    "cracking": "master_cracking_10m",
    "roughness_parent": "master_roughness_parent",
    "roughness_child_10m": "master_roughness_child_10m",
    "roughness_child_100m": "master_roughness_as_per_ca",
    "rutting_parent": "master_rutting_parent",
    "rutting_child_10m": "master_rutting_child_10m",
    "rutting_child_100m": "master_rutting_as_per_ca",
    "ravelling_parent": "master_ravelling",
    "ravelling_child_10m": "master_ravelling_10m",
    "ravelling_child_100m": "master_ravelling",
}

BASE_DEFECT_COLUMNS = FINAL_TABLE_SCHEMAS.get("master_cracking_10m", [])


def normalize_col_name(name: str) -> str:
    return str(name).strip().lower()


def enforce_table_schema(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Enforce exact column order, column count, and default values matching target table schema.
    """
    if df is None or table_name not in FINAL_TABLE_SCHEMAS:
        return df

    target_cols = FINAL_TABLE_SCHEMAS[table_name]
    out = df.copy()

    # Column aliases for specific defects if needed
    if "rutting_avg" in target_cols and "rutting_avg" not in out.columns:
        if "metric_value" in out.columns:
            out["rutting_avg"] = out["metric_value"]
        elif "rutting_mm" in out.columns:
            out["rutting_avg"] = out["rutting_mm"]

    if "ravelling_area" in target_cols and "ravelling_area" not in out.columns:
        if "area_sqm" in out.columns:
            out["ravelling_area"] = out["area_sqm"]

    if "cracking_pct" in target_cols and "cracking_pct" not in out.columns:
        if "metric_value" in out.columns:
            out["cracking_pct"] = out["metric_value"]

    # Fill in any missing target columns with None
    for c in target_cols:
        if c not in out.columns:
            out[c] = None

    # Keep only the target columns in exact defined order
    out = out[target_cols]
    return out


class TableSchemaManager:
    """
    Manages loading and enforcement of table schemas.
    """

    def __init__(self, workbook_path: Optional[str] = None):
        self.workbook_path = workbook_path
        self.schemas = FINAL_TABLE_SCHEMAS

    def get_columns(self, table_name: str) -> List[str]:
        return self.schemas.get(table_name, list(BASE_DEFECT_COLUMNS))

    def enforce_schema(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        return enforce_table_schema(df, table_name)
