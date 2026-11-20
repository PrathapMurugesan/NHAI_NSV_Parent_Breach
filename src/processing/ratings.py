"""
NH standardization and rating calculation module.
Ported from Databricks DLM Extraction Pipeline (Cell 12).

Standardizes NH number format and fills rating_ca and rating_irc independently
across twin CA and IRC rows.
"""

from typing import Dict, Any, Optional
import re
import pandas as pd
import numpy as np

from src.common.constants import (
    standardize_nh_number,
    CA_CRACK_PCT,
    CA_BI_THRESHOLD,
    CA_RUT_THRESHOLD,
    RAVELLING_RATING_THRESHOLD,
)
from src.common.logger import get_logger

logger = get_logger(__name__)


def compute_ca_rating_label(defect: str, metric_value: Any, bi_value: Any = None, thresholds: Optional[Dict[str, float]] = None) -> Optional[str]:
    """
    Compute CA rating label for a specific defect based on metric value.
    """
    if metric_value is None or pd.isna(metric_value):
        return None
    try:
        v = float(metric_value)
    except (ValueError, TypeError):
        return None

    th = thresholds or {}

    if defect == 'cracking':
        t = th.get('cracking', CA_CRACK_PCT)
        b2, b3, b4 = round(t * 2), round(t * 4), round(t * 6)
        if v < t:
            return f"Very Good (<{t:.0f}%)"
        elif v <= b2:
            return f"Good ({t:.0f}-{b2}%)"
        elif v <= b3:
            return f"Fair ({b2}-{b3}%)"
        elif v <= b4:
            return f"Poor ({b3}-{b4}%)"
        else:
            return f"Very Poor (>{b4}%)"

    elif defect == 'roughness':
        t = th.get('roughness', CA_BI_THRESHOLD)
        fl = round(t * 0.8)
        p = None
        if bi_value is not None and not pd.isna(bi_value):
            try:
                p = float(bi_value)
            except (ValueError, TypeError):
                p = None
        if p is None:
            p = v * 1000.0 if v < 50.0 else v  # heuristic if IRI vs BI
        if p < fl:
            return f"Good (<{fl}mm/km)"
        elif p <= t:
            return f"Fair ({fl}-{t:.0f}mm/km)"
        else:
            return f"Poor (>{t:.0f}mm/km)"

    elif defect == 'rutting':
        t = th.get('rutting', CA_RUT_THRESHOLD)
        fl = round(t * 0.5)
        if v < fl:
            return f"Good (<{fl}mm)"
        elif v <= t:
            return f"Fair ({fl}-{t:.0f}mm)"
        else:
            return f"Poor (>{t:.0f}mm)"

    elif defect == 'ravelling':
        t = th.get('ravelling', float(RAVELLING_RATING_THRESHOLD))
        h = round(t * 0.5)
        if v == 0:
            return "Very Good (0)"
        elif v <= h:
            return f"Good (1-{h}%)"
        elif v < t:
            return f"Fair ({h}-{t:.0f}%)"
        else:
            return f"Poor (>={t:.0f}%)"

    return None


def poor_rating_label(defect: str, ca_threshold: Optional[float]) -> Optional[str]:
    """
    The 'Poor' band label of a defect (what a record is rated when it is a breach only because an "as per CA"
    child breaches, while its own average value would rate Good / Very Good). None when there is no threshold.
    """
    if ca_threshold is None or pd.isna(ca_threshold):
        return None
    t = float(ca_threshold)
    probe = {'roughness': t + 1.0, 'rutting': t + 1.0, 'ravelling': t, 'cracking': round(t * 4) + 1.0}.get(defect)
    if probe is None:
        return None
    return compute_ca_rating_label(defect, probe, probe if defect == 'roughness' else None, {defect: t})


def standardize_and_rate(
    output_dfs: Dict[str, pd.DataFrame],
    thresholds: Optional[Dict[str, float]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Standardize NH numbers and independently fill rating_ca and rating_irc.
    Twin rows (CA and IRC) are paired by base id to ensure both rows carry both ratings.
    """
    res = {}
    th = thresholds or {
        'cracking': CA_CRACK_PCT,
        'roughness': CA_BI_THRESHOLD,
        'rutting': CA_RUT_THRESHOLD,
        'ravelling': float(RAVELLING_RATING_THRESHOLD),
    }

    for tbl, df in output_dfs.items():
        if df is None or df.empty:
            res[tbl] = df
            continue

        out = df.copy()

        # Standardize NH number
        if 'nh_number' in out.columns:
            out['nh_number'] = out['nh_number'].apply(standardize_nh_number)

        if tbl == 'master_pothole':
            if 'severity' in out.columns:
                out['rating_ca'] = out.get('rating_ca', pd.Series(dtype=object)).fillna(out['severity'])
                out['rating_irc'] = out.get('rating_irc', pd.Series(dtype=object)).fillna(out['severity'])
            res[tbl] = out
            continue

        defect = next((d for d in ['cracking', 'roughness', 'rutting', 'ravelling'] if d in tbl.lower()), None)
        if defect is None:
            res[tbl] = out
            continue

        std_col = out['threshold_name'].fillna('').astype(str).str.lower()
        is_ca = std_col.str.contains('ca')
        is_irc = std_col.str.contains('irc')
        has_data = out['metric_value'].notna()
        breach = pd.to_numeric(out.get('breach_flag', 0), errors='coerce').fillna(0).astype(int) == 1

        ca_ratings = []
        for idx, row in out.iterrows():
            if is_ca.iloc[idx]:
                mv = row.get('metric_value')
                bi = row.get('roughness_bi') if 'roughness_bi' in row else None
                lbl = compute_ca_rating_label(defect, mv, bi, th)
                if lbl is None:
                    lbl = row.get('rating_ca')
                if lbl is None or pd.isna(lbl):
                    lbl = row.get('severity')
                if (lbl is None or pd.isna(lbl)) and has_data.iloc[idx]:
                    lbl = 'Bad' if breach.iloc[idx] else 'Good'
                ca_ratings.append(lbl)
            else:
                ca_ratings.append(None)

        irc_ratings = []
        for idx, row in out.iterrows():
            if is_irc.iloc[idx]:
                lbl = row.get('road_condition')
                if (lbl is None or pd.isna(lbl)) and has_data.iloc[idx]:
                    lbl = 'Poor' if breach.iloc[idx] else 'Good'
                irc_ratings.append(lbl)
            else:
                irc_ratings.append(None)

        out['_ca_own'] = ca_ratings
        out['_irc_own'] = irc_ratings

        # Twin rows matching key: id with '_irc_' replaced by '_'
        id_col = 'id' if 'id' in out.columns else out.columns[0]
        out['_pair_key'] = out[id_col].astype(str).str.replace('_irc_', '_', regex=False)

        # Broadcast max non-null across the twin pair
        ca_map = out.dropna(subset=['_ca_own']).groupby('_pair_key')['_ca_own'].first().to_dict()
        irc_map = out.dropna(subset=['_irc_own']).groupby('_pair_key')['_irc_own'].first().to_dict()

        out['rating_ca'] = out['_pair_key'].map(ca_map)
        out['rating_irc'] = out['_pair_key'].map(irc_map)

        out.drop(columns=['_ca_own', '_irc_own', '_pair_key'], inplace=True, errors='ignore')
        res[tbl] = out
        logger.debug("Standardized NH and filled ratings for %s (%d rows)", tbl, len(out))

    return res
