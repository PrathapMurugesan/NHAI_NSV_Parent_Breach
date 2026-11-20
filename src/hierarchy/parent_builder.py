"""
Side parent builder module (LHS/RHS).
Ported from Databricks DLM Extraction Pipeline (Cell 14).

Builds aggregated 1000m parent records for LHS and RHS across lanes
for linear defects (Roughness, Rutting).

Roughness (one record per side per 1000m):
  - Pool every 10m 'As per CA' row of all lanes on the side (L* -> LHS, R* -> RHS)
  - Side IRI = (mean LWP IRI + mean RWP IRI) / 2
  - Side BI  = 630 x (Side IRI)^1.12
  - breach_flag / affected_length_ca / rating_ca / road_condition: Side BI >= CA limit
    OR any 10m 'As per CA' child of the side in that 1000m breaches (see below)
  - affected_length_irc / rating_irc: Side BI >= IRC limit OR any 10m 'As per IRC' child breaches
    (same record, no IRC row)
  - iri_value, roughness_bi and metric_value all carry the side values
Rutting (one record per side per 1000m), same pattern:
  - Side rut = (mean Left + mean Right) / 2 over the side's pooled 10m rows
    (mean of the 10m average rut depth when Left/Right are missing)
  - CA breach / IRC columns on the same record, as for roughness

Breach follows the children: a side parent (0-1000m) is a breach when its value is above the limit OR when
any one 10m child of that side and 1000m is a breach - one breaching 10m child is enough, even when the
average of the parent stays below the limit. CA and IRC are decided separately, each from its own 10m rows.
"""

from typing import Dict, Any, List, Optional, Tuple
import json
import pandas as pd
import numpy as np

from src.common.constants import (
    PARENT_LENGTH_M,
    TABLE_SPEC,
    has_parent,
    parent_len,
    normalize_lane,
    lane_to_direction,
)
from src.common.logger import get_logger
from src.processing.ratings import compute_ca_rating_label

logger = get_logger(__name__)


def _side_of_lane(lane_series: pd.Series) -> np.ndarray:
    norm = lane_series.apply(normalize_lane).astype(str).str.upper()
    return np.where(norm.str.startswith('L'), 'LHS', np.where(norm.str.startswith('R'), 'RHS', None))


def child_breach_bins(src_10m: Optional[pd.DataFrame], threshold_name: str, parent_len_m: int) -> set:
    """
    {(side, bin_start)} of the side parents that have at least one breaching 10m child
    (breach_flag == 1 on a 10m row of that standard, in any lane of the side).
    """
    if src_10m is None or src_10m.empty or 'breach_flag' not in src_10m.columns:
        return set()
    df = src_10m
    if 'threshold_name' in df.columns:
        df = df[df['threshold_name'] == threshold_name]
    df = df[pd.to_numeric(df['breach_flag'], errors='coerce').fillna(0).astype(int) == 1]
    if df.empty:
        return set()
    side = _side_of_lane(df['lane'])
    lo = np.minimum(pd.to_numeric(df['start_chainage'], errors='coerce'),
                    pd.to_numeric(df['end_chainage'], errors='coerce'))
    b = np.floor(lo / parent_len_m) * parent_len_m
    return {(sd, int(x)) for sd, x in zip(side, b) if sd is not None and pd.notna(x)}


def pool_side_roughness(src_10m: Optional[pd.DataFrame], parent_len_m: int) -> Dict[Tuple[str, int], Dict[str, Any]]:
    """
    Pool all 10m 'As per CA' roughness rows of a side within each parent bin.
    Returns {(side, bin_start): {lwp_mean, rwp_mean, iri, bi, rows, iri_source}}.
    """
    if src_10m is None or src_10m.empty:
        return {}
    df = src_10m.copy()
    if 'threshold_name' in df.columns:
        df = df[df['threshold_name'] == 'As per CA']
    if df.empty:
        return {}

    def _m(v):
        try:
            return json.loads(v) if isinstance(v, str) else (v if isinstance(v, dict) else {})
        except Exception:
            return {}

    meas = df['measures'].apply(_m) if 'measures' in df.columns else pd.Series([{}] * len(df), index=df.index)
    df['_lwp'] = pd.to_numeric(df['lwp_iri'] if 'lwp_iri' in df.columns else meas.apply(lambda m: m.get('lwp_iri')), errors='coerce')
    df['_rwp'] = pd.to_numeric(df['rwp_iri'] if 'rwp_iri' in df.columns else meas.apply(lambda m: m.get('rwp_iri')), errors='coerce')
    df['_iri'] = pd.to_numeric(df.get('iri_value'), errors='coerce')
    df['_side'] = _side_of_lane(df['lane'])
    s_col = pd.to_numeric(df['start_chainage'], errors='coerce')
    e_col = pd.to_numeric(df['end_chainage'], errors='coerce')
    df['_b'] = np.floor(np.minimum(s_col, e_col) / parent_len_m) * parent_len_m
    df = df.dropna(subset=['_side', '_b'])

    pooled = {}
    for (side, b), g in df.groupby(['_side', '_b']):
        lwp, rwp = g['_lwp'].mean(), g['_rwp'].mean()
        if pd.notna(lwp) and pd.notna(rwp):
            iri, src = (lwp + rwp) / 2.0, 'mean LWP + mean RWP'
        elif g['_iri'].notna().any():
            iri, src = g['_iri'].mean(), 'mean lane IRI (wheel paths missing)'
        else:
            iri, src = None, 'no IRI data'
        pooled[(side, int(b))] = {
            'lwp_mean': None if pd.isna(lwp) else float(lwp),
            'rwp_mean': None if pd.isna(rwp) else float(rwp),
            'iri': None if iri is None else float(iri),
            'bi': None if iri is None else float(630.0 * (iri ** 1.12)),
            'rows': int(len(g)),
            'iri_source': src,
        }
    return pooled


def pool_side_rutting(src_10m: Optional[pd.DataFrame], parent_len_m: int) -> Dict[Tuple[str, int], Dict[str, Any]]:
    """
    Pool all 10m 'As per CA' rutting rows of a side within each parent bin.
    Returns {(side, bin_start): {left_mean, right_mean, value, rows, value_source}}.
    """
    if src_10m is None or src_10m.empty:
        return {}
    df = src_10m.copy()
    if 'threshold_name' in df.columns:
        df = df[df['threshold_name'] == 'As per CA']
    if df.empty:
        return {}

    def _m(v):
        try:
            return json.loads(v) if isinstance(v, str) else (v if isinstance(v, dict) else {})
        except Exception:
            return {}

    meas = df['measures'].apply(_m) if 'measures' in df.columns else pd.Series([{}] * len(df), index=df.index)
    df['_l'] = pd.to_numeric(meas.apply(lambda m: m.get('rut_left_mm')), errors='coerce')
    df['_r'] = pd.to_numeric(meas.apply(lambda m: m.get('rut_right_mm')), errors='coerce')
    avg_src = df['rutting_avg'] if 'rutting_avg' in df.columns else df.get('metric_value')
    df['_avg'] = pd.to_numeric(avg_src, errors='coerce')
    df['_side'] = _side_of_lane(df['lane'])
    s_col = pd.to_numeric(df['start_chainage'], errors='coerce')
    e_col = pd.to_numeric(df['end_chainage'], errors='coerce')
    df['_b'] = np.floor(np.minimum(s_col, e_col) / parent_len_m) * parent_len_m
    df = df.dropna(subset=['_side', '_b'])

    pooled = {}
    for (side, b), g in df.groupby(['_side', '_b']):
        lm, rm = g['_l'].mean(), g['_r'].mean()
        if pd.notna(lm) and pd.notna(rm):
            val, src = (lm + rm) / 2.0, 'mean Left + mean Right'
        elif g['_avg'].notna().any():
            val, src = g['_avg'].mean(), 'mean 10m average rut depth (Left/Right missing)'
        else:
            val, src = None, 'no rutting data'
        pooled[(side, int(b))] = {
            'left_mean': None if pd.isna(lm) else float(lm),
            'right_mean': None if pd.isna(rm) else float(rm),
            'value': None if val is None else float(val),
            'rows': int(len(g)),
            'value_source': src,
        }
    return pooled


def build_side_parent(
    defect: str,
    src_df: pd.DataFrame,
    upc: str,
    ca_threshold: float,
    parent_len_m: int = PARENT_LENGTH_M,
    src_10m: Optional[pd.DataFrame] = None,
    irc_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    Build LHS / RHS 1000m parent table for a given defect from its 1km CA rows.
    Roughness and rutting: side value pooled from the side's 10m rows (see module docstring).
    """
    if not has_parent(defect) or src_df is None or src_df.empty:
        return pd.DataFrame()

    table, L = TABLE_SPEC[defect]['parent']
    L = parent_len_m

    # Filter to 'As per CA'
    df = src_df.copy()
    if 'threshold_name' in df.columns:
        df = df[df['threshold_name'] == 'As per CA']

    if df.empty:
        logger.warning("No CA rows found for %s parent aggregation", defect)
        return pd.DataFrame(columns=src_df.columns)

    cols = list(src_df.columns)

    # Determine side (LHS for L-lanes, RHS for R-lanes)
    df['_norm_lane'] = df['lane'].apply(normalize_lane)
    df['_side'] = np.where(
        df['_norm_lane'].str.upper().str.startswith('L'),
        'LHS',
        np.where(df['_norm_lane'].str.upper().str.startswith('R'), 'RHS', None)
    )
    df = df.dropna(subset=['_side'])

    s_col = pd.to_numeric(df['start_chainage'], errors='coerce')
    e_col = pd.to_numeric(df['end_chainage'], errors='coerce')
    df['_lo'] = np.minimum(s_col, e_col)
    df['_hi'] = np.maximum(s_col, e_col)
    df['_b'] = (np.floor(df['_lo'] / L) * L).astype(int)

    if defect == 'roughness':
        pooled = pool_side_roughness(src_10m, L)
    elif defect == 'rutting':
        pooled = pool_side_rutting(src_10m, L)
    else:
        pooled = {}

    ca_child_hits = child_breach_bins(src_10m, 'As per CA', L)
    irc_child_hits = child_breach_bins(src_10m, 'As per IRC', L)

    rows = []
    grouped = df.groupby(['_side', '_b'])

    for (side, b), g in grouped:
        b = int(b)
        idx_min = g['_lo'].idxmin()
        idx_max = g['_hi'].idxmax()
        first = g.loc[idx_min]
        last = g.loc[idx_max]

        mv = pd.to_numeric(g['metric_value'], errors='coerce')
        lane_bf = pd.to_numeric(g['breach_flag'], errors='coerce').fillna(0).astype(int)
        child_ca = (side, b) in ca_child_hits
        child_irc = (side, b) in irc_child_hits
        bf = int(lane_bf.max() == 1 or child_ca)

        worst = g.loc[mv.idxmax()] if mv.notna().any() else first
        pav = sorted(set(g['pavement_type'].dropna()))

        pid = f"{upc}_{side}_{b}_{b + L}"

        def _sum(c: str) -> float:
            if c in g.columns:
                return float(pd.to_numeric(g[c], errors='coerce').fillna(0).sum())
            return 0.0

        rec = {c: first.get(c) for c in cols}
        rec.update({
            'id': f"{upc}_{table}_{side}_{b}",
            'collection_name': table,
            'level': 'parent',
            'lane': side,
            'side': side,
            'direction': 'Increasing' if side == 'LHS' else 'Decreasing',
            'parent_id': pid,
            'record_id': pid,
            'source_defect_id': pid,
            'defect_id': pid if bf else None,
            'display_defect_id': None,
            'metric_value': round(float(mv.mean()), 6) if mv.notna().any() else None,
            'breach_flag': bf,
            'threshold_name': 'As per CA',
            'threshold_value': float(ca_threshold),
            'total_children': int(len(g)),
            'segment_count': int(_sum('segment_count')),
            'start_chainage': int(first['_lo']),
            'end_chainage': int(last['_hi']),
            'length_m': float(last['_hi'] - first['_lo']),
            'total_length': _sum('length_m'),
            'defect_chainage': (float(first['_lo']) + float(last['_hi'])) / 2.0,
            'start_lat': first.get('start_lat'),
            'start_long': first.get('start_long'),
            'end_lat': last.get('end_lat'),
            'end_long': last.get('end_long'),
            'defect_lat': first.get('start_lat'),
            'defect_long': first.get('start_long'),
            'pavement_type': pav[0] if len(pav) == 1 else ('Hybrid' if pav else None),
            'parent_start_chainage': b,
            'parent_end_chainage': b + L,
            'reporting_interval_m': L,
            'rating_ca': worst.get('rating_ca'),
            'rating_irc': worst.get('rating_irc'),
            'road_condition': 'Bad' if bf else 'Good',
            'affected_length': _sum('affected_length'),
            'affected_length_ca': _sum('affected_length'),
            'affected_length_irc': 0.0,
            'measures': json.dumps({
                'side': side,
                'lanes': sorted(g['lane'].astype(str)),
                'lane_values': {
                    str(l): (None if pd.isna(v) else round(float(v), 6))
                    for l, v in zip(g['lane'], mv)
                },
                'lane_breach': {str(l): int(x) for l, x in zip(g['lane'], lane_bf)},
                'calculation': (
                    f"{side} {b}-{b + L}: value = average of {len(g)} lane {L}m values; "
                    f"breach if any lane breaches the CA threshold ({ca_threshold})"
                ),
            }),
        })

        if defect == 'roughness' and (side, b) in pooled:
            pr = pooled[(side, b)]
            side_bi = pr['bi']
            metric_ca = side_bi is not None and side_bi >= float(ca_threshold)
            metric_irc = side_bi is not None and irc_threshold is not None and side_bi >= float(irc_threshold)
            bf_ca = int(metric_ca or child_ca)
            bf_irc = int(metric_irc or child_irc)
            tot = _sum('length_m')
            rec.update({
                'iri_value': pr['iri'],
                'roughness_bi': side_bi,
                'metric_value': None if side_bi is None else round(side_bi, 6),
                'breach_flag': bf_ca,
                'defect_id': pid if bf_ca else None,
                'rating_ca': compute_ca_rating_label('roughness', side_bi, side_bi, {'roughness': float(ca_threshold)}),
                'rating_irc': None if (side_bi is None and not bf_irc) else ('Poor' if bf_irc else 'Good'),
                'road_condition': 'Bad' if bf_ca else 'Good',
                'affected_length': tot if bf_ca else 0.0,
                'affected_length_ca': tot if bf_ca else 0.0,
                'affected_length_irc': tot if bf_irc else 0.0,
                'measures': json.dumps({
                    'side': side,
                    'lanes': sorted(g['lane'].astype(str)),
                    'side_lwp_iri_mean': pr['lwp_mean'],
                    'side_rwp_iri_mean': pr['rwp_mean'],
                    'side_iri': pr['iri'],
                    'side_bi': side_bi,
                    'pooled_10m_rows': pr['rows'],
                    'iri_source': pr['iri_source'],
                    'ca_threshold': float(ca_threshold),
                    'irc_threshold': None if irc_threshold is None else float(irc_threshold),
                    'breach_ca': bf_ca,
                    'breach_irc': bf_irc,
                    'breach_from_value_ca': int(metric_ca),
                    'breach_from_child_ca': int(child_ca),
                    'breach_from_value_irc': int(metric_irc),
                    'breach_from_child_irc': int(child_irc),
                    'lane_bi_values': {
                        str(l): (None if pd.isna(v) else round(float(v), 6))
                        for l, v in zip(g['lane'], mv)
                    },
                    'calculation': (
                        f"{side} {b}-{b + L}: Side IRI = (mean LWP + mean RWP)/2 over {pr['rows']} pooled 10m rows; "
                        f"Side BI = 630 x IRI^1.12; CA breach if BI >= {ca_threshold} or any 10m child breaches"
                        + (f"; IRC breach if BI >= {irc_threshold} or any 10m child breaches" if irc_threshold is not None else "")
                    ),
                }),
            })

        if defect == 'rutting' and (side, b) in pooled:
            pr = pooled[(side, b)]
            val = pr['value']
            metric_ca = val is not None and val >= float(ca_threshold)
            metric_irc = val is not None and irc_threshold is not None and val >= float(irc_threshold)
            bf_ca = int(metric_ca or child_ca)
            bf_irc = int(metric_irc or child_irc)
            tot = _sum('length_m')
            rec.update({
                'rutting_avg': val,
                'metric_value': None if val is None else round(val, 6),
                'breach_flag': bf_ca,
                'defect_id': pid if bf_ca else None,
                'rating_ca': compute_ca_rating_label('rutting', val, None, {'rutting': float(ca_threshold)}),
                'rating_irc': None if (val is None and not bf_irc) else ('Poor' if bf_irc else 'Good'),
                'road_condition': 'Bad' if bf_ca else 'Good',
                'affected_length': tot if bf_ca else 0.0,
                'affected_length_ca': tot if bf_ca else 0.0,
                'affected_length_irc': tot if bf_irc else 0.0,
                'measures': json.dumps({
                    'side': side,
                    'lanes': sorted(g['lane'].astype(str)),
                    'side_rut_left_mean_mm': pr['left_mean'],
                    'side_rut_right_mean_mm': pr['right_mean'],
                    'side_rutting_avg_mm': val,
                    'pooled_10m_rows': pr['rows'],
                    'value_source': pr['value_source'],
                    'ca_threshold': float(ca_threshold),
                    'irc_threshold': None if irc_threshold is None else float(irc_threshold),
                    'breach_ca': bf_ca,
                    'breach_irc': bf_irc,
                    'breach_from_value_ca': int(metric_ca),
                    'breach_from_child_ca': int(child_ca),
                    'breach_from_value_irc': int(metric_irc),
                    'breach_from_child_irc': int(child_irc),
                    'lane_values': {
                        str(l): (None if pd.isna(v) else round(float(v), 6))
                        for l, v in zip(g['lane'], mv)
                    },
                    'calculation': (
                        f"{side} {b}-{b + L}: Side rut = (mean Left + mean Right)/2 over {pr['rows']} pooled 10m rows; "
                        f"CA breach if >= {ca_threshold} or any 10m child breaches"
                        + (f"; IRC breach if >= {irc_threshold} or any 10m child breaches" if irc_threshold is not None else "")
                    ),
                }),
            })

        if 'area_sqm' in cols:
            rec['area_sqm'] = _sum('area_sqm')
        if 'cracking_pct' in cols:
            rec['cracking_pct'] = rec['metric_value']

        rows.append(rec)

    parent_df = pd.DataFrame(rows)
    logger.info("Built side parent table %s: %d rows", table, len(parent_df))
    return parent_df
