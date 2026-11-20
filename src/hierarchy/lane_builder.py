"""
Lane-wise 1000m table builder (master_{defect}_1000m_lane).

The side parents (LHS / RHS) pool all lanes of a side into one record per km. These tables keep the lanes
apart: one record per lane (L1, L2, ... R1, R2, ...) per 1000m, for roughness, rutting, cracking and
ravelling (no pothole). The rows come from the lane-wise 1000m aggregate that is already built for each
defect (CA values, as for the side parents); here they get their own identity and the IRC columns.

  - id / parent_id / record_id : {upc}_{lane}_{start}_{end}   (one record per lane per 1000m)
  - breach_flag / affected_length_ca : the lane's 1000m value is at or above the CA limit OR any one
    "as per CA" child of that lane in those 1000m is a breach (master_<defect>_as_per_ca, the child at the CA
    reporting interval - not the 10m rows), even when the lane's 1000m value stays below the limit
  - a row that is a breach only through a child gets road_condition 'Bad' and the 'Poor' rating_ca (its own
    value could still rate Good / Very Good)
  - affected_length_irc / rating_irc : lane value >= IRC limit on the same record (no IRC row, as for the side
    parents); rating_irc is set for roughness and rutting only
  - display_defect_id is not set here: assign_missing_display_ids() numbers the breach rows afterwards
"""

from typing import Optional, Tuple
import json
import re
import numpy as np
import pandas as pd

from src.common.constants import (
    PARENT_LENGTH_M,
    lane_1000m_table,
    lane_to_direction,
    normalize_lane,
)
from src.common.logger import get_logger
from src.processing.ratings import poor_rating_label

logger = get_logger(__name__)

DISPLAY_ID_RE = re.compile(r"^D/(\d{6})/N/(\d{5})(?:/\d{4})?$")     # D/<ddmmyy>/N/<nnnnn> (and its /<nnnn> children)


def max_display_seq(*frames: Optional[pd.DataFrame]) -> int:
    """Highest top counter (the nnnnn) among the display_defect_id of the given tables (0 when there is none)."""
    top = 0
    for df in frames:
        if df is None or df.empty or 'display_defect_id' not in df.columns:
            continue
        for v in df['display_defect_id'].dropna().astype(str):
            m = DISPLAY_ID_RE.match(v)
            if m:
                top = max(top, int(m.group(2)))
    return top


def assign_missing_display_ids(df: pd.DataFrame, date_str: str, start_seq: int) -> Tuple[pd.DataFrame, int]:
    """
    Breach rows (breach_flag == 1) that have no display_defect_id get D/<date>/N/<seq:05d>, numbered from
    start_seq in side / lane / chainage order. A row that already has an ID keeps it - nothing is renumbered.
    Returns (table, number of IDs assigned).
    """
    out = df.copy()
    if 'display_defect_id' not in out.columns:
        out['display_defect_id'] = None
    has = out['display_defect_id'].notna() & (out['display_defect_id'].astype(str) != '')
    need = (pd.to_numeric(out['breach_flag'], errors='coerce').fillna(0).astype(int) == 1) & ~has
    n = int(need.sum())
    if not n:
        return out, 0
    order = out[need].copy()
    col = lambda c, fill: order[c].fillna(fill) if c in order.columns else fill
    order['_s'], order['_l'], order['_i'] = col('side', 'ZZZ'), col('lane', 'ZZZ'), col('id', '')
    order['_c'] = pd.to_numeric(order['start_chainage'], errors='coerce').fillna(999999999)
    order = order.sort_values(['_s', '_l', '_c', '_i'], kind='stable')
    ids = {i: f"D/{date_str}/N/{start_seq + k:05d}" for k, i in enumerate(order.index)}
    out.loc[order.index, 'display_defect_id'] = pd.Series(ids)
    return out, n


def child_breach_lane_bins(children: Optional[pd.DataFrame], threshold_name: str, parent_len_m: int) -> set:
    """
    {(lane, bin_start)} of the lanes that have at least one breaching "as per CA" child in that 1000m
    (breach_flag == 1 on a row of master_<defect>_as_per_ca).
    """
    if children is None or children.empty or 'breach_flag' not in children.columns:
        return set()
    df = children
    if 'threshold_name' in df.columns:
        df = df[df['threshold_name'] == threshold_name]
    df = df[pd.to_numeric(df['breach_flag'], errors='coerce').fillna(0).astype(int) == 1]
    if df.empty:
        return set()
    lane = df['lane'].apply(normalize_lane)
    lo = np.minimum(pd.to_numeric(df['start_chainage'], errors='coerce'),
                    pd.to_numeric(df['end_chainage'], errors='coerce'))
    b = np.floor(lo / parent_len_m) * parent_len_m
    return {(ln, int(x)) for ln, x in zip(lane, b) if ln is not None and pd.notna(x)}


def stamp_measures(raw, child_ca: bool, value_ca: bool, bf_ca: int):
    """measures JSON of a lane record with where its breach came from (unchanged when it is not valid JSON)."""
    try:
        m = json.loads(raw) if isinstance(raw, str) else {}
    except Exception:
        return raw
    if not isinstance(m, dict):
        return raw
    m.update(breach_ca=int(bf_ca), breach_from_value_ca=int(value_ca), breach_from_child_ca=int(child_ca))
    return json.dumps(m)


def build_lane_1000m(
    defect: str,
    src_df: Optional[pd.DataFrame],
    upc: str,
    irc_threshold: Optional[float] = None,
    parent_len_m: int = PARENT_LENGTH_M,
    ca_children: Optional[pd.DataFrame] = None,
    ca_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    Lane-wise 1000m table of a defect from its lane-wise 1000m aggregate (empty frame when there is none).
    ca_children: the defect's "as per CA" child table, for the breach that follows the children.
    ca_threshold: the CA limit, to name the 'Poor' rating of a row that breaches only through a child.
    """
    table = lane_1000m_table(defect)
    if table is None or src_df is None or src_df.empty:
        return pd.DataFrame()

    df = src_df.copy()
    if 'threshold_name' in df.columns:
        df = df[df['threshold_name'] == 'As per CA'].copy()
    if df.empty:
        return pd.DataFrame()

    L = int(parent_len_m)
    df['lane'] = df['lane'].apply(normalize_lane)
    s = pd.to_numeric(df['start_chainage'], errors='coerce')
    e = pd.to_numeric(df['end_chainage'], errors='coerce')
    df['_lo'] = np.minimum(s, e)
    df['_hi'] = np.maximum(s, e)
    df = df.dropna(subset=['lane', '_lo']).copy()
    if df.empty:
        return pd.DataFrame()

    b = (np.floor(df['_lo'] / L) * L).astype(int)
    pid = pd.Series([f"{upc}_{ln}_{x}_{x + L}" for ln, x in zip(df['lane'], b)], index=df.index)
    mv = pd.to_numeric(df.get('metric_value'), errors='coerce')
    length = pd.to_numeric(df.get('length_m'), errors='coerce')
    length = length.fillna(df['_hi'] - df['_lo'])
    value_ca = pd.to_numeric(df.get('breach_flag'), errors='coerce').fillna(0).astype(int) == 1
    hits = child_breach_lane_bins(ca_children, 'As per CA', L)
    child_ca = pd.Series([(ln, int(x)) in hits for ln, x in zip(df['lane'], b)], index=df.index)
    bf = (value_ca | child_ca).astype(int)

    df['id'] = [f"{upc}_{table}_{ln}_{x}" for ln, x in zip(df['lane'], b)]
    df['collection_name'] = table
    df['level'] = 'lane_1000m'
    df['standard'] = 'CA'
    df['parent_id'] = pid
    df['record_id'] = pid
    df['source_defect_id'] = pid
    df['defect_id'] = np.where(bf == 1, pid, None)
    df['display_defect_id'] = None                         # numbered with assign_missing_display_ids afterwards
    df['side'] = np.where(df['lane'].str.startswith('L'), 'LHS', np.where(df['lane'].str.startswith('R'), 'RHS', None))
    df['direction'] = df['lane'].apply(lane_to_direction)
    df['start_chainage'] = df['_lo'].astype(int)
    df['end_chainage'] = df['_hi'].astype(int)
    df['length_m'] = length
    df['parent_start_chainage'] = b
    df['parent_end_chainage'] = b + L
    df['reporting_interval_m'] = L
    df['breach_flag'] = bf
    df['affected_length_ca'] = np.where(bf == 1, length, 0.0)
    df['affected_length'] = df['affected_length_ca']

    if irc_threshold is not None:
        bf_irc = (mv >= float(irc_threshold)).fillna(False)
        df['affected_length_irc'] = np.where(bf_irc, length, 0.0)
        if defect in ('roughness', 'rutting'):
            df['rating_irc'] = np.where(mv.isna(), None, np.where(bf_irc, 'Poor', 'Good'))

    from_child = (bf == 1) & ~value_ca                     # a breach only through an "as per CA" child
    if from_child.any():
        df.loc[from_child, 'road_condition'] = 'Bad'
        poor = poor_rating_label(defect, ca_threshold)
        if poor:
            df.loc[from_child, 'rating_ca'] = poor
    if 'measures' in df.columns:
        df['measures'] = [stamp_measures(m, c, v, f) for m, c, v, f in zip(df['measures'], child_ca, value_ca, bf)]

    df = df.drop(columns=['_lo', '_hi'])
    df = df.sort_values(['lane', 'start_chainage'], kind='stable').reset_index(drop=True)
    logger.info("Built lane-wise 1000m table %s: %d rows (%d lanes)", table, len(df), df['lane'].nunique())
    return df
