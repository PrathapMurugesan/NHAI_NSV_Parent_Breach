"""DQ checks V10 - V12 of the extraction and the per-UPC run log (dlm_logs)."""
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.output.run_logs import RunLog                       # noqa: E402
from src.validation.validator import Validator               # noqa: E402


def _lane(flag_l1=1, cond_l1='Bad'):
    return pd.DataFrame({
        'id': ['t_L1_0', 't_L2_0'], 'lane': ['L1', 'L2'], 'side': ['LHS', 'LHS'],
        'start_chainage': [0, 0], 'end_chainage': [1000, 1000], 'parent_start_chainage': [0, 0],
        'parent_id': ['U1_P2_C1_L1_0_1000', 'U1_P2_C1_L2_0_1000'], 'length_m': [1000.0, 1000.0],
        'breach_flag': [flag_l1, 0], 'defect_id': ['U1_P2_C1_L1_0_1000' if flag_l1 else None, None],
        'display_defect_id': ['D/201126/N/00001' if flag_l1 else None, None],
        'road_condition': [cond_l1, 'Good'], 'rating_ca': ['Poor (20-30%)', 'Very Good (<5%)'], 'threshold_value': [5.0, 5.0],
        'affected_length_ca': [1000.0 if flag_l1 else 0.0, 0.0],
        'measures': [json.dumps({'breach_from_child_ca': 1, 'breach_from_value_ca': 0}), json.dumps({})],
    })


def _children():
    return pd.DataFrame([dict(lane='L1', start_chainage=500, end_chainage=600, threshold_name='As per CA', breach_flag=1)])


def _run(lane):
    v = Validator({'master_cracking_1000m_lane': lane, 'master_cracking_as_per_ca': _children()}, {}, 'U1', 'B', '2', 1)
    v._v10_lane()
    v._v11_breach_rule()
    v._v12_ids()
    return pd.DataFrame(v.val_rows)


class TestSilverDQ(unittest.TestCase):

    def test_correct_lane_table_passes(self):
        r = _run(_lane())
        self.assertNotIn('FAIL', set(r['status']))
        self.assertTrue({'V10 Lane', 'V11 Breach rule', 'V12 ID counter'} <= set(r['section']))

    def test_child_breach_not_flagged_fails(self):
        r = _run(_lane(flag_l1=0))
        bad = r[(r['check'].str.startswith('a breaching as-per-CA child')) & (r['status'] == 'FAIL')]
        self.assertEqual(len(bad), 1)

    def test_child_only_breach_that_is_not_bad_fails(self):
        r = _run(_lane(cond_l1='Good'))
        self.assertIn('child-only breach has road_condition Bad', set(r[r['status'] == 'FAIL']['check']))


class TestRunLog(unittest.TestCase):

    def _settings(self, tmp):
        return SimpleNamespace(upc='U1', zone='B', phase='2', cycle='1',
                               silver_output_dir=os.path.join(tmp, 'UPC=U1', 'dlm_kpi_new'))

    def test_log_and_summary_written(self):
        tmp = tempfile.mkdtemp()
        with RunLog(self._settings(tmp)) as run:
            import logging
            logging.getLogger('dlm_pipeline').info('hello from the run')
            run.result = {'status': 'SUCCESS', 'tables': ['a', 'b'], 'rows': 7, 'validation_summary': {'PASS': 5, 'WARN': 1}}
        d = os.path.join(tmp, 'UPC=U1', 'dlm_logs')
        names = os.listdir(d)
        log = next(n for n in names if n.endswith('.log'))
        self.assertIn('hello from the run', open(os.path.join(d, log), encoding='utf-8').read())
        s = pd.read_csv(os.path.join(d, 'run_summary.csv'))
        self.assertEqual((s.loc[0, 'status'], int(s.loc[0, 'dq_pass']), int(s.loc[0, 'dq_warn']), int(s.loc[0, 'rows'])), ('SUCCESS', 5, 1, 7))

    def test_failed_run_is_logged_and_error_propagates(self):
        tmp = tempfile.mkdtemp()
        with self.assertRaises(ValueError):
            with RunLog(self._settings(tmp)):
                raise ValueError('boom')
        s = pd.read_csv(os.path.join(tmp, 'UPC=U1', 'dlm_logs', 'run_summary.csv'))
        self.assertEqual(s.loc[0, 'status'], 'FAILED')
        self.assertIn('boom', s.loc[0, 'error'])


if __name__ == '__main__':
    unittest.main()
