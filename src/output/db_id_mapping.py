"""
Project-level ID columns of the Silver tables (project_id, piu_id, zone_id, state_id, region_id, contractor_id, ae_ie_id),
filled from the UPC's row in Postgres (master_upc_project) before the Silver -> DB push.

Why: the extraction has no access to the master tables, and the push script sets an unknown foreign-key value to NULL
(so the column ends up empty in the DB). These values are taken from Postgres itself, so they are valid by construction.

Mapping (nothing is guessed; what cannot be mapped is reported as UNMAPPED):
  1. foreign-key metadata of the DB: a column of the target table that references master_upc_project takes that
     column of the UPC's project row; a column that references another master table (e.g. master_piu.id) takes the
     column of the project row that references the same table
  2. else the same-named column of the project row (also nsv_<name>, and 'id' for project_id)
Pure functions - no DB access here.
"""
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

ID_NAMES = ["project_id", "piu_id", "zone_id", "state_id", "region_id", "contractor_id", "ae_ie_id"]
PROJECT_TABLE = "master_upc_project"
Fk = Tuple[str, str, str, str]                      # (column, ref_schema, ref_table, ref_col)


def blank(v: Any) -> bool:
    return v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() in ("", "nan", "None", "null", "NaT")


def derive_sources(project_row: Dict[str, Any], table_fks: List[Fk], project_fks: List[Fk],
                   table_cols: List[str]) -> Dict[str, Dict[str, Any]]:
    """{column: {value, how, source}} for every project-level ID column of the table (value None when unmapped)."""
    fk_by_col = {c: (rt, rc) for c, _, rt, rc in table_fks}
    pfk_by_ref = {(rt, rc): c for c, _, rt, rc in project_fks}
    out: Dict[str, Dict[str, Any]] = {}
    for col in table_cols:
        ref = fk_by_col.get(col)
        project_level = col in ID_NAMES or (ref is not None and ref[0] == PROJECT_TABLE)
        if not project_level:
            continue
        value, how, src = None, "unmapped", ""
        if ref is not None:
            if ref[0] == PROJECT_TABLE and not blank(project_row.get(ref[1])):
                value, how, src = project_row[ref[1]], "foreign key", f"{PROJECT_TABLE}.{ref[1]}"
            elif ref in pfk_by_ref and not blank(project_row.get(pfk_by_ref[ref])):
                pc = pfk_by_ref[ref]
                value, how, src = project_row[pc], "foreign key", f"{PROJECT_TABLE}.{pc}"
        if value is None:
            for cand in (col, f"nsv_{col}", "id" if col == "project_id" else None):
                if cand and cand in project_row and not blank(project_row[cand]):
                    value, how, src = project_row[cand], "same name", f"{PROJECT_TABLE}.{cand}"
                    break
        out[col] = dict(value=None if value is None else (value if isinstance(value, (int, float)) else str(value)),
                        how=how, source=src)
    return out


def populate_df(df: pd.DataFrame, sources: Dict[str, Dict[str, Any]]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Fill the ID columns of a Silver table from the mapped project values.
    A blank value is filled; a different value is replaced by the Postgres one; an equal value is left.
    Returns (table, one stat row per column: rows, blank_before, filled, replaced, blank_after, status).
    """
    out = df.copy()
    stats: List[Dict[str, Any]] = []
    for col, s in sources.items():
        if col not in out.columns:
            continue
        cur = out[col]
        isblank = cur.map(blank)
        n = len(out)
        st = dict(column=col, rows=n, blank_before=int(isblank.sum()), source=s["source"], how=s["how"],
                  db_value=s["value"], filled=0, replaced=0)
        v = s["value"]
        if v is None:
            st.update(blank_after=int(isblank.sum()), status="UNMAPPED" if n else "EMPTY TABLE")
        else:
            diff = ~isblank & (cur.astype(str) != str(v))
            st.update(filled=int(isblank.sum()), replaced=int(diff.sum()))
            out[col] = v if len(out) else cur
            if not out[col].map(lambda x: isinstance(x, str)).all() and isinstance(v, str):
                out[col] = out[col].astype(object)
            st["blank_after"] = int(out[col].map(blank).sum())
            st["status"] = ("OK" if not (st["filled"] or st["replaced"]) else
                            "FILLED" if not st["replaced"] else "REPLACED")
        stats.append(st)
    return out, stats
