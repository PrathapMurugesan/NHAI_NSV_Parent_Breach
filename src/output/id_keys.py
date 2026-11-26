"""
Phase / cycle in the record IDs.

The record IDs of the Silver tables are built from the UPC alone (N_07024_01001_GJ_master_roughness_as_per_ca_000001), so the
same UPC in another phase or cycle produces the same IDs - they are not unique (the DB primary key is `id`). Every ID that
starts with the UPC now carries the phase and cycle:

    N_07024_01001_GJ_master_roughness_as_per_ca_000001      ->  N_07024_01001_GJ_P1_C1_master_roughness_as_per_ca_000001
    N_07024_01001_GJ_LHS_0_1000                              ->  N_07024_01001_GJ_P1_C1_LHS_0_1000
    D/110125/N/N_07024_01001_GJ/70/Cracking/0008             ->  D/110125/N/N_07024_01001_GJ_P1_C1/70/Cracking/0008

The same rule is applied to every ID column of every table, so the links stay intact (child parent_id = parent defect_id,
record_id, source_defect_id, geometry ids). display_defect_id (D/<ddmmyy>/N/<nnnnn>), the upc / upc_code columns and the
other fields are not touched. Applying it again changes nothing.
"""
import re
from typing import Dict, Tuple

import pandas as pd

ID_COLUMNS = ("id", "parent_id", "defect_id", "record_id", "source_defect_id", "child_id", "layer_id")


def run_key(upc: str, phase, cycle) -> str:
    """N_07024_01001_GJ + phase 1 + cycle 1 -> N_07024_01001_GJ_P1_C1"""
    return f"{upc}_P{phase}_C{cycle}"


def _rewriter(upc: str, key: str):
    plain = re.compile(rf"^{re.escape(upc)}_(?!P\d+_C\d+_)")                         # <upc>_<rest> (not yet keyed)
    disp = re.compile(rf"^(D/\d{{6}}/N/){re.escape(upc)}/")                          # D/<date>/N/<upc>/<rest>

    def fix(v):
        if not isinstance(v, str):
            return v
        if v.startswith(upc + "_"):
            return plain.sub(key + "_", v, count=1)
        m = disp.match(v)
        if m:
            return m.group(1) + key + "/" + v[m.end():]
        return v
    return fix


def apply_run_key(outputs: Dict[str, pd.DataFrame], upc: str, phase, cycle) -> Tuple[Dict[str, pd.DataFrame], Dict[str, int]]:
    """Insert _P<phase>_C<cycle> after the UPC in the ID columns of every table. Returns (tables, ids changed per table)."""
    key = run_key(upc, phase, cycle)
    fix = _rewriter(upc, key)
    out, changed = {}, {}
    for name, df in outputs.items():
        if df is None or df.empty:
            out[name] = df
            continue
        df = df.copy()
        n = 0
        for col in ID_COLUMNS:
            if col in df.columns and (df[col].dtype == object or pd.api.types.is_string_dtype(df[col])):
                new = df[col].map(fix)
                n += int((new.astype(str) != df[col].astype(str)).sum())
                df[col] = new
        out[name] = df
        changed[name] = n
    return out, changed
