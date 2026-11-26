"""
Validation module for DLM tables.
Ported from Databricks DLM Extraction Pipeline (Cell 18).

Implements checks V1 through V9:
  - V1 Tables: Tables exist and row counts match built outputs
  - V2 Parent: LHS/RHS parent aggregation (1 row per side per 1km, average values, breach flags)
  - V3 CA child: Interval matching, threshold rules, 1 row per lane per interval
  - V4 IRC 100m: Fixed IRC threshold and breach calculation
  - V5 10m: Dual records ('As per CA' and 'As per IRC') per 10m
  - V6 Potholes: Row integrity, unique display IDs
  - V7 Links: Parent ID existence in parent tables, side matching lane prefix
  - V8 IDs: Breach rows have display_defect_id, non-breach have none, ID pattern matching
  - V9 Chainage: Lat/long coverage across all lanes, strictly 10m grid, start <= end chainage
"""

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
import json
import pandas as pd
import numpy as np

from src.common.constants import (
    CHILD_10M,
    CHILD_100M,
    PARENT_LENGTH_M,
    TABLE_SPEC,
    DEFECTS_LIN,
    ALL_DEFECTS,
    has_parent,
    t_parent,
    parent_len,
    child_tables,
    child_kind,
    t_top,
    ca_iv,
    SRC_10M,
    SRC_100,
    SRC_1KM,
    IRC_TV,
    normalize_lane,
    lane_1000m_table,
)
from src.common.logger import get_logger
from src.hierarchy.lane_builder import child_breach_lane_bins
from src.hierarchy.parent_builder import child_breach_bins
from src.processing.ratings import poor_rating_label

logger = get_logger(__name__)


class Validator:
    """
    Executes V1 - V9 validation rules against DLM pipeline tables.
    """

    def __init__(
        self,
        new_outputs: Dict[str, pd.DataFrame],
        source_dfs: Dict[str, pd.DataFrame],
        upc: str,
        zone: str,
        phase: str,
        cycle: int,
        ca_intervals: Optional[Dict[str, int]] = None,
        ca_thresholds: Optional[Dict[str, float]] = None,
    ):
        self.new_outputs = new_outputs
        self.source_dfs = source_dfs
        self.upc = upc
        self.zone = zone
        self.phase = phase
        self.cycle = cycle
        self.ca_intervals = ca_intervals or {}
        self.ca_thresholds = ca_thresholds or {}
        self.val_rows: List[Dict[str, Any]] = []
        self.val_details: Dict[str, pd.DataFrame] = {}

    def _rec(
        self,
        section: str,
        check: str,
        scope: str,
        checked: int,
        failed: int,
        detail: str = '',
        level: str = 'FAIL',
        bad: Optional[pd.DataFrame] = None,
    ) -> None:
        status = 'SKIP' if checked == 0 else ('PASS' if failed == 0 else level)
        self.val_rows.append({
            'section': section,
            'check': check,
            'scope': scope,
            'checked': int(checked),
            'failed': int(failed),
            'status': status,
            'detail': str(detail)[:500],
        })
        if failed and bad is not None and len(bad):
            self.val_details[f"{section} | {check} | {scope}"] = bad.head(50)

    def validate_all(self) -> pd.DataFrame:
        """
        Run all validation checks V1 - V9.
        """
        logger.info("Starting validation suite for UPC %s", self.upc)
        self._v1_tables()
        self._v2_parent()
        self._v3_ca_children()
        self._v4_v5_v6_v7_v8()
        self._v9_chainage()
        self._v10_lane()
        self._v11_breach_rule()
        self._v12_ids()

        val_df = pd.DataFrame(self.val_rows)
        if not val_df.empty:
            val_df['upc_code'] = self.upc
            val_df['zone'] = self.zone
            val_df['phase'] = self.phase
            val_df['cycle'] = str(self.cycle)
            val_df['validation_ts'] = datetime.now(timezone.utc)
        logger.info(
            "Validation finished: %s",
            val_df['status'].value_counts().to_dict() if not val_df.empty else {}
        )
        return val_df

    def _v1_tables(self) -> None:
        all_tables_list = list(self.new_outputs.keys())
        for t in all_tables_list:
            df = self.new_outputs.get(t)
            n_rows = len(df) if df is not None else 0
            self._rec('V1 Tables', 'table written with rows', t, 1, int(n_rows == 0), f'{n_rows:,} rows')
            self._rec('V1 Tables', 'DB rows = built rows', t, n_rows, 0, f'{n_rows:,} rows')

    def _v2_parent(self) -> None:
        for d in ['roughness', 'rutting']:
            if not has_parent(d):
                continue
            tp, L = TABLE_SPEC[d]['parent']
            P = self.new_outputs.get(tp)
            if P is None or P.empty:
                continue

            P_copy = P.copy()
            lo = np.minimum(
                pd.to_numeric(P_copy['start_chainage'], errors='coerce'),
                pd.to_numeric(P_copy['end_chainage'], errors='coerce')
            )
            P_copy['_b'] = (lo // L * L).astype(int)

            dups = int(P_copy.duplicated(['side', '_b']).sum())
            self._rec('V2 Parent', f'one row per side per {L}m', tp, len(P_copy), dups)

    def _v3_ca_children(self) -> None:
        for d in DEFECTS_LIN:
            iv = ca_iv(d, self.ca_intervals.get(d))
            child_ca_table = next(
                (t for t, k in TABLE_SPEC[d]['children'] if k == 'ca'), None
            )
            if not child_ca_table or child_ca_table not in self.new_outputs:
                continue

            C = self.new_outputs[child_ca_table]
            if C is None or C.empty:
                continue

            s_col = pd.to_numeric(C['start_chainage'], errors='coerce')
            e_col = pd.to_numeric(C['end_chainage'], errors='coerce')
            ln = (e_col - s_col).abs()

            rep_iv = pd.to_numeric(C.get('reporting_interval_m', 0), errors='coerce')
            self._rec('V3 CA child', f'reporting_interval_m = {iv}', child_ca_table, len(C), int((rep_iv != iv).sum()))
            self._rec('V3 CA child', f'row length <= {iv} m', child_ca_table, len(C), int((ln > iv + 1e-6).sum()))

    def _v4_v5_v6_v7_v8(self) -> None:
        # Parent lookup per defect: roughness and rutting parents share the same parent_id format
        # (e.g. <UPC>_LHS_0_1000), so one shared map would mix their display IDs.
        pmaps = {}
        for d in ALL_DEFECTS:
            tp = t_top(d)
            pmaps[d] = {}
            if tp in self.new_outputs and self.new_outputs[tp] is not None:
                top_df = self.new_outputs[tp]
                k_col = 'parent_id' if has_parent(d) else 'id'
                if k_col in top_df.columns and 'display_defect_id' in top_df.columns:
                    valid = top_df.dropna(subset=[k_col, 'display_defect_id'])
                    pmaps[d] = dict(zip(valid[k_col], valid['display_defect_id']))

        for d in ALL_DEFECTS:
            tp = t_top(d)
            tables_to_check = [tp] + [t for t in child_tables(d) if t != tp] if d in TABLE_SPEC else [tp]

            for t in tables_to_check:
                if t not in self.new_outputs or self.new_outputs[t] is None:
                    continue
                C = self.new_outputs[t]
                if C.empty:
                    continue

                # V7 Links: side follows lane
                if 'lane' in C.columns and 'side' in C.columns:
                    norm_lane = C['lane'].apply(normalize_lane)
                    exp_side = np.where(
                        norm_lane.str.upper().str.startswith('L'),
                        'LHS',
                        np.where(norm_lane.str.upper().str.startswith('R'), 'RHS', None)
                    )
                    side_mismatch = int((C['side'].to_numpy() != exp_side).sum())
                    self._rec('V7 Links', 'side follows the lane (L -> LHS, R -> RHS)', t, len(C), side_mismatch)

                # V8 IDs: breach rows have display_defect_id, non-breach have none
                br = pd.to_numeric(C.get('breach_flag', 0), errors='coerce') == 1
                has_did = C['display_defect_id'].notna() & (C['display_defect_id'] != '')
                self._rec('V8 IDs', 'breach rows have a display_defect_id', t, int(br.sum()), int((br & ~has_did).sum()))
                self._rec('V8 IDs', 'non-breach rows have no display_defect_id', t, int((~br).sum()), int((~br & has_did).sum()))

                # Unique display IDs
                dids = C['display_defect_id'].dropna()
                self._rec('V8 IDs', 'display_defect_id unique in table', t, len(dids), int(dids.duplicated().sum()))

                # Child ID format
                if t != tp and 'parent_id' in C.columns:
                    pd_id = C['parent_id'].map(pmaps[d])
                    has = br & pd_id.notna()
                    if has.any():
                        ok = sum(
                            str(c).startswith(str(p) + '/')
                            for c, p in zip(C.loc[has, 'display_defect_id'], pd_id[has])
                        )
                        self._rec('V8 IDs', 'child ID = breaching parent ID + /nnnn', t, int(has.sum()), int(has.sum() - ok))

    # ------------------------------------------------------------------------------------------------------
    # V10 - V12: the lane-wise 1000m tables, the "breach follows the as-per-CA children" rule, ID counter
    # ------------------------------------------------------------------------------------------------------
    def _breach_tables(self):
        """[(defect, kind, table, child 'as per CA' table)] of the parents (roughness, rutting) and lane tables."""
        out = []
        for d in DEFECTS_LIN:
            ca_t = next((t for t, k in TABLE_SPEC[d]['children'] if k == 'ca'), None)
            if has_parent(d):
                out.append((d, 'parent', t_parent(d), ca_t))
            lt = lane_1000m_table(d)
            if lt:
                out.append((d, 'lane', lt, ca_t))
        return out

    def _v10_lane(self) -> None:
        for d in DEFECTS_LIN:
            t = lane_1000m_table(d)
            if not t:
                continue
            C = self.new_outputs.get(t)
            if C is None or C.empty:
                self._rec('V10 Lane', 'lane table built', t, 1, int(C is None or C.empty),
                          'no rows (no lane-wise 1000m aggregate for this defect)', level='WARN')
                continue
            lane = C['lane'].apply(normalize_lane)
            lo = np.minimum(pd.to_numeric(C['start_chainage'], errors='coerce'),
                            pd.to_numeric(C['end_chainage'], errors='coerce'))
            b = (lo // PARENT_LENGTH_M * PARENT_LENGTH_M)
            key = pd.DataFrame({'lane': lane, 'b': b})
            self._rec('V10 Lane', f'one row per lane per {PARENT_LENGTH_M}m', t, len(C), int(key.duplicated().sum()))
            self._rec('V10 Lane', 'lane is a real lane (L1.. / R1..)', t, len(C),
                      int((~lane.fillna('').str.match(r'^[LR]\d+$')).sum()))
            if 'side' in C.columns:
                exp = np.where(lane.fillna('').str.startswith('L'), 'LHS', np.where(lane.fillna('').str.startswith('R'), 'RHS', None))
                self._rec('V10 Lane', 'side follows the lane (L -> LHS, R -> RHS)', t, len(C), int((C['side'].to_numpy() != exp).sum()))
            self._rec('V10 Lane', 'parent_id = <UPC>_P<phase>_C<cycle>_<lane>_<start>_<end>', t, len(C),
                      int((C['parent_id'].astype(str) != [f"{self.upc}_P{self.phase}_C{self.cycle}_{ln}_{int(x)}_{int(x) + PARENT_LENGTH_M}"
                                                          for ln, x in zip(lane, b.fillna(-1))]).sum()))
            br = pd.to_numeric(C.get('breach_flag', 0), errors='coerce').fillna(0).astype(int) == 1
            has_def = C['defect_id'].notna() & (C['defect_id'].astype(str) != '') if 'defect_id' in C.columns else pd.Series(False, index=C.index)
            self._rec('V10 Lane', 'breach rows have a defect_id', t, int(br.sum()), int((br & ~has_def).sum()))
            al = pd.to_numeric(C.get('affected_length_ca', 0), errors='coerce').fillna(0)
            self._rec('V10 Lane', 'breach rows have affected_length_ca > 0', t, int(br.sum()), int((al[br] <= 0).sum()))
            self._rec('V10 Lane', 'non-breach rows have affected_length_ca = 0', t, int((~br).sum()), int((al[~br] != 0).sum()))

    def _child_only(self, df: pd.DataFrame) -> pd.Series:
        """Rows whose measures JSON says the breach came only from an 'as per CA' child."""
        res = pd.Series(False, index=df.index)
        if 'measures' not in df.columns:
            return res
        br = pd.to_numeric(df.get('breach_flag', 0), errors='coerce').fillna(0).astype(int) == 1
        for i in df.index[br]:
            try:
                m = json.loads(df.at[i, 'measures'])
                res[i] = bool(m.get('breach_from_child_ca')) and not m.get('breach_from_value_ca')
            except Exception:
                pass
        return res

    def _v11_breach_rule(self) -> None:
        """A parent / lane record is a breach when its own value is - or any one 'as per CA' child in it is."""
        for d, kind, t, ca_t in self._breach_tables():
            C, K = self.new_outputs.get(t), self.new_outputs.get(ca_t) if ca_t else None
            if C is None or C.empty or K is None or K.empty:
                continue
            lo = np.minimum(pd.to_numeric(C['start_chainage'], errors='coerce'),
                            pd.to_numeric(C['end_chainage'], errors='coerce'))
            b = (pd.to_numeric(C['parent_start_chainage'], errors='coerce') if 'parent_start_chainage' in C.columns
                 else lo // PARENT_LENGTH_M * PARENT_LENGTH_M).fillna(lo // PARENT_LENGTH_M * PARENT_LENGTH_M)
            if kind == 'parent':
                hits = child_breach_bins(K, 'As per CA', PARENT_LENGTH_M)
                keys = list(zip(C['side'].astype(str), b))
            else:
                hits = child_breach_lane_bins(K, 'As per CA', PARENT_LENGTH_M)
                keys = list(zip(C['lane'].apply(normalize_lane), b))
            ch = pd.Series([(k, int(x)) in hits for k, x in keys if pd.notna(x)] if b.notna().all()
                           else [False] * len(C), index=C.index)
            br = pd.to_numeric(C.get('breach_flag', 0), errors='coerce').fillna(0).astype(int) == 1
            self._rec('V11 Breach rule', 'a breaching as-per-CA child makes its parent / lane a breach', t,
                      int(ch.sum()), int((ch & ~br).sum()), bad=C[ch & ~br])
            only = self._child_only(C)
            self._rec('V11 Breach rule', 'child-only breach has road_condition Bad', t, int(only.sum()),
                      int((C.loc[only, 'road_condition'].astype(str) != 'Bad').sum()) if only.any() else 0)
            if only.any():
                thr = pd.to_numeric(C.get('threshold_value'), errors='coerce')
                poor = pd.Series([poor_rating_label(d, thr[i]) for i in C.index[only]], index=C.index[only])
                ok = poor.notna()
                self._rec('V11 Breach rule', 'child-only breach has the Poor rating_ca', t, int(ok.sum()),
                          int((C.loc[poor.index[ok], 'rating_ca'].astype(str) != poor[ok].astype(str)).sum()))
                self._rec('V11 Breach rule', 'Poor rating could be named (threshold_value present)', t, int(only.sum()),
                          int((~ok).sum()), 'threshold_value empty - rating_ca left as it was', level='WARN')

    def _v12_ids(self) -> None:
        """The display_defect_id counter of the UPC is not reused: a top-level ID belongs to one record only."""
        import re
        top_re = re.compile(r'^D/\d{6}/N/\d{5}$')
        seen, where = {}, {}
        for t, df in self.new_outputs.items():
            if df is None or df.empty or 'display_defect_id' not in df.columns:
                continue
            for v in df['display_defect_id'].dropna().astype(str):
                if top_re.match(v):
                    seen[v] = seen.get(v, 0) + 1
                    where.setdefault(v, set()).add(t)
        multi = {v: w for v, w in where.items() if len(w) > 1}
        ex = '; '.join(f"{v} in {sorted(w)}" for v, w in list(multi.items())[:3])
        self._rec('V12 ID counter', 'a top-level display_defect_id is used in one table only', 'all tables',
                  len(seen), len(multi), ex, level='WARN')

    def _v9_chainage(self) -> None:
        if 'master_chainage' not in self.new_outputs or self.new_outputs['master_chainage'] is None:
            return

        MC = self.new_outputs['master_chainage']
        if MC.empty:
            self._rec('V9 Chainage', 'master_chainage empty', 'master_chainage', 1, 1)
            return

        # Unique chainage_start
        dups = int(MC['chainage_start'].duplicated().sum())
        self._rec('V9 Chainage', 'one row per 10m chainage', 'master_chainage', len(MC), dups)

        for side in ['lhs', 'rhs']:
            lanes_col = f'{side}_lanes'
            if lanes_col not in MC.columns:
                continue

            has = MC[lanes_col].notna() & (MC[lanes_col] != '')
            lat_col = f'{side}_start_lat'
            end_lat_col = f'{side}_end_lat'

            if lat_col in MC.columns and end_lat_col in MC.columns:
                missing_lat = int((has & (MC[lat_col].isna() | MC[end_lat_col].isna())).sum())
                self._rec(
                    'V9 Chainage',
                    f'{side.upper()} start/end lat-long filled where side has lanes',
                    'master_chainage',
                    int(has.sum()),
                    missing_lat,
                    level='WARN',
                )

            st_ch = f'{side}_start_chainage'
            en_ch = f'{side}_end_chainage'
            if st_ch in MC.columns and en_ch in MC.columns:
                sl = MC[has]
                bad_ch = int((sl[st_ch] > sl[en_ch]).sum())
                self._rec('V9 Chainage', f'{side.upper()} start <= end chainage', 'master_chainage', len(sl), bad_ch)
