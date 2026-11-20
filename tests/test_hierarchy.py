"""
Unit tests for hierarchy layer: LHS/RHS parent aggregation, child records, and display IDs.
"""

import unittest
import pandas as pd
import numpy as np

from src.hierarchy.parent_builder import build_side_parent
from src.hierarchy.child_builder import build_child_10m
from src.hierarchy.lhs_rhs_builder import assign_display_defect_ids
from src.hierarchy.chainage_builder import build_master_chainage


class TestHierarchy(unittest.TestCase):

    def test_side_parent_builder(self):
        # 1km CA rows for L1 and L2
        src_df = pd.DataFrame({
            'lane': ['L1', 'L2'],
            'start_chainage': [0, 0],
            'end_chainage': [1000, 1000],
            'metric_value': [1500.0, 2500.0],
            'breach_flag': [0, 1],
            'threshold_name': ['As per CA', 'As per CA'],
            'pavement_type': ['Bituminous', 'Bituminous'],
            'length_m': [1000.0, 1000.0],
            'segment_count': [1, 1],
            'start_lat': [12.0, 12.0],
            'start_long': [77.0, 77.0],
            'end_lat': [12.01, 12.01],
            'end_long': [77.01, 77.01],
        })

        parent_df = build_side_parent(
            defect='roughness',
            src_df=src_df,
            upc='UPC001',
            ca_threshold=2000.0,
            parent_len_m=1000,
        )

        self.assertEqual(len(parent_df), 1)
        row = parent_df.iloc[0]
        self.assertEqual(row['side'], 'LHS')
        self.assertEqual(row['lane'], 'LHS')
        # Average of 1500 and 2500 = 2000
        self.assertAlmostEqual(row['metric_value'], 2000.0)
        # One lane breached -> parent breached
        self.assertEqual(row['breach_flag'], 1)
        self.assertEqual(row['road_condition'], 'Bad')
        self.assertEqual(row['parent_id'], 'UPC001_LHS_0_1000')

    def test_side_parent_breach_follows_children(self):
        CA, IRC = 2000.0, 2400.0
        bi = lambda iri: 630 * iri ** 1.12
        spikes = {('L1', 500): 3.2,      # BI ~2300: breaches CA only
                  ('R1', 200): 3.5}      # BI ~2540: breaches CA and IRC
        rows = []
        for lane in ('L1', 'L2', 'R1'):
            for st in range(0, 2000, 10):
                iri = spikes.get((lane, st), 1.0)
                for std, thr in (('CA', CA), ('IRC', IRC)):
                    rows.append(dict(lane=lane, start_chainage=st, end_chainage=st + 10, iri_value=iri, lwp_iri=iri,
                                     rwp_iri=iri, metric_value=bi(iri), breach_flag=int(bi(iri) >= thr),
                                     threshold_name=f'As per {std}'))
        src_10m = pd.DataFrame(rows)
        src_df = pd.DataFrame([
            dict(lane=l, start_chainage=s, end_chainage=s + 1000, metric_value=700.0, breach_flag=0,
                 threshold_name='As per CA', length_m=1000.0, segment_count=100, pavement_type='Bituminous')
            for l in ('L1', 'L2', 'R1') for s in (0, 1000)])

        out = build_side_parent('roughness', src_df, 'U1', CA, 1000, src_10m=src_10m, irc_threshold=IRC)
        get = lambda side, start: out[(out['side'] == side) & (out['start_chainage'] == start)].iloc[0]

        lhs0 = get('LHS', 0)                      # side average is far below the limit, one 10m child breaches
        self.assertLess(lhs0['metric_value'], CA)
        self.assertEqual(lhs0['breach_flag'], 1)
        self.assertEqual(lhs0['road_condition'], 'Bad')
        self.assertGreater(lhs0['affected_length_ca'], 0)
        self.assertEqual(lhs0['affected_length_irc'], 0)          # the child breaches CA only
        self.assertEqual(get('LHS', 1000)['breach_flag'], 0)       # no breaching child, low average -> no breach
        rhs0 = get('RHS', 0)
        self.assertEqual(rhs0['breach_flag'], 1)
        self.assertGreater(rhs0['affected_length_irc'], 0)         # IRC decided from its own 10m rows
        self.assertEqual(rhs0['rating_irc'], 'Poor')
        self.assertEqual(get('RHS', 1000)['breach_flag'], 0)

    def test_lane_1000m_builder(self):
        from src.hierarchy.lane_builder import build_lane_1000m
        from config.table_schema import enforce_table_schema, FINAL_TABLE_SCHEMAS

        src_df = pd.DataFrame({
            'lane': ['L1', 'L2', 'R1', 'L1'],
            'start_chainage': [0, 0, 1000, 1000],
            'end_chainage': [1000, 1000, 2000, 2000],
            'metric_value': [1500.0, 2500.0, 2600.0, 100.0],
            'breach_flag': [0, 1, 1, 0],
            'threshold_name': ['As per CA'] * 3 + ['As per IRC'],      # the IRC row is not a lane record
            'length_m': [1000.0, 1000.0, 1000.0, 1000.0],
        })
        out = build_lane_1000m('roughness', src_df, 'UPC001', irc_threshold=2400.0)

        self.assertEqual(len(out), 3)                                   # lanes kept apart, no LHS/RHS pooling
        self.assertEqual(sorted(out['lane']), ['L1', 'L2', 'R1'])
        l2 = out[out['lane'] == 'L2'].iloc[0]
        self.assertEqual(l2['id'], 'UPC001_master_roughness_1000m_lane_L2_0')
        self.assertEqual(l2['parent_id'], 'UPC001_L2_0_1000')
        self.assertEqual(l2['collection_name'], 'master_roughness_1000m_lane')
        self.assertEqual(l2['side'], 'LHS')
        self.assertEqual(l2['direction'], 'Increasing')
        self.assertEqual(l2['affected_length_ca'], 1000.0)
        self.assertEqual(l2['affected_length_irc'], 1000.0)             # 2500 >= 2400
        self.assertEqual(l2['rating_irc'], 'Poor')
        l1 = out[out['lane'] == 'L1'].iloc[0]
        self.assertEqual(l1['affected_length_ca'], 0.0)
        self.assertEqual(l1['rating_irc'], 'Good')
        self.assertEqual(out[out['lane'] == 'R1'].iloc[0]['side'], 'RHS')

        # no pothole table, empty source -> nothing
        self.assertTrue(build_lane_1000m('pothole', src_df, 'UPC001').empty)
        self.assertTrue(build_lane_1000m('rutting', None, 'UPC001').empty)

        # schema of the new table is the parent's schema
        for d in ('roughness', 'rutting', 'cracking', 'ravelling'):
            self.assertIn(f'master_{d}_1000m_lane', FINAL_TABLE_SCHEMAS)
        enforced = enforce_table_schema(out, 'master_roughness_1000m_lane')
        self.assertEqual(list(enforced.columns), FINAL_TABLE_SCHEMAS['master_roughness_1000m_lane'])

    def test_lane_1000m_breach_follows_children(self):
        from src.hierarchy.lane_builder import build_lane_1000m

        src_df = pd.DataFrame({
            'lane': ['L1', 'L2', 'R1'], 'start_chainage': [0, 0, 0], 'end_chainage': [1000, 1000, 1000],
            'metric_value': [700.0, 700.0, 700.0], 'breach_flag': [0, 0, 0],       # every lane average is below the limit
            'threshold_name': ['As per CA'] * 3, 'length_m': [1000.0] * 3, 'road_condition': ['Good'] * 3,
        })
        rows = []
        for lane in ('L1', 'L2', 'R1'):
            for st in range(0, 1000, 10):
                spike = (lane == 'L1' and st == 500)                                # one breaching 10m child on L1
                for std in ('CA', 'IRC'):
                    rows.append(dict(lane=lane, start_chainage=st, end_chainage=st + 10, threshold_name=f'As per {std}',
                                     breach_flag=int(spike and std == 'CA')))     # breaches CA, not IRC
        children = pd.DataFrame(rows)

        out = build_lane_1000m('roughness', src_df, 'U1', irc_threshold=2400.0, src_10m=children)
        get = lambda lane: out[out['lane'] == lane].iloc[0]
        self.assertEqual(get('L1')['breach_flag'], 1)                              # a child breaches -> the lane breaches
        self.assertEqual(get('L1')['defect_id'], 'U1_L1_0_1000')
        self.assertEqual(get('L1')['affected_length_ca'], 1000.0)
        self.assertEqual(get('L1')['road_condition'], 'Bad')
        self.assertEqual(get('L1')['affected_length_irc'], 0.0)                    # the child breaches CA only
        self.assertEqual(get('L1')['rating_irc'], 'Good')
        self.assertEqual(get('L2')['breach_flag'], 0)                              # no breaching child, low average
        self.assertEqual(get('L2')['road_condition'], 'Good')
        self.assertEqual(get('R1')['breach_flag'], 0)                              # the child of L1 does not count for R1

        # without the 10m table the lane keeps its own value-based flag
        self.assertEqual(build_lane_1000m('roughness', src_df, 'U1').iloc[0]['breach_flag'], 0)

    def test_missing_display_ids_are_generated_existing_kept(self):
        from src.hierarchy.lane_builder import assign_missing_display_ids, max_display_seq

        df = pd.DataFrame({
            'id': ['a', 'b', 'c', 'd'], 'lane': ['L1', 'L2', 'L3', 'R1'], 'start_chainage': [0, 0, 0, 0],
            'breach_flag': [1, 0, 1, 1],
            'display_defect_id': ['D/201126/N/00004', None, None, ''],      # L1 has one, L2 is no breach, L3 / R1 miss it
        })
        self.assertEqual(max_display_seq(df), 4)
        out, n = assign_missing_display_ids(df, '201126', 5)
        self.assertEqual(n, 2)
        self.assertEqual(out['display_defect_id'].tolist(),
                         ['D/201126/N/00004', None, 'D/201126/N/00005', 'D/201126/N/00006'])
        again, n2 = assign_missing_display_ids(out, '201126', 7)               # nothing left to generate
        self.assertEqual(n2, 0)
        self.assertEqual(again['display_defect_id'].tolist(), out['display_defect_id'].tolist())

    def test_display_defect_id_assignment(self):
        # Parent table with 1 breach
        parent_df = pd.DataFrame({
            'id': ['P1'],
            'parent_id': ['PID1'],
            'side': ['LHS'],
            'lane': ['LHS'],
            'start_chainage': [0],
            'breach_flag': [1],
        })
        # Child table: 2 breaches linked to P1, 1 non-breach, 1 orphan breach
        child_df = pd.DataFrame({
            'id': ['C1', 'C2', 'C3', 'C4'],
            'parent_id': ['PID1', 'PID1', 'PID1', 'ORPHAN_PID'],
            'side': ['LHS', 'LHS', 'LHS', 'RHS'],
            'lane': ['L1', 'L2', 'L1', 'R1'],
            'start_chainage': [10, 20, 30, 100],
            'breach_flag': [1, 1, 0, 1],
        })

        tables = {
            'master_roughness_parent': parent_df,
            'master_roughness_child_10m': child_df,
        }

        numbered = assign_display_defect_ids(tables, date_str="181126", starting_seq=0)

        # Parent gets D/181126/N/00001
        p_res = numbered['master_roughness_parent']
        self.assertEqual(p_res['display_defect_id'].iloc[0], "D/181126/N/00001")

        # Child rows linked to parent get D/181126/N/00001/0001 and 0002
        c_res = numbered['master_roughness_child_10m']
        c1 = c_res[c_res['id'] == 'C1']['display_defect_id'].iloc[0]
        c2 = c_res[c_res['id'] == 'C2']['display_defect_id'].iloc[0]
        c3 = c_res[c_res['id'] == 'C3']['display_defect_id'].iloc[0]
        c4 = c_res[c_res['id'] == 'C4']['display_defect_id'].iloc[0]

        self.assertEqual(c1, "D/181126/N/00001/0001")
        self.assertEqual(c2, "D/181126/N/00001/0002")
        self.assertIsNone(c3)
        # Orphan breach gets next top counter: D/181126/N/00002
        self.assertEqual(c4, "D/181126/N/00002")

    def test_master_chainage_builder(self):
        # 10m roughness CA rows for L1 and R1
        rough_10m = pd.DataFrame({
            'lane': ['L1', 'R1'],
            'start_chainage': [0, 0],
            'end_chainage': [10, 10],
            'threshold_name': ['As per CA', 'As per CA'],
            'start_lat': [12.0, 12.0],
            'start_long': [77.0, 77.0],
            'end_lat': [12.0001, 12.0001],
            'end_long': [77.0001, 77.0001],
            'pavement_type': ['Bituminous', 'Bituminous'],
            'nh_number': ['NH-27', 'NH-27'],
            'section_code': ['SEC01', 'SEC01'],
            'carriageway': ['Dual', 'Dual'],
        })

        output_dfs = {'master_roughness_child_10m': rough_10m}
        mc = build_master_chainage(
            output_dfs,
            upc='UPC001',
            upc_slash='UPC/001',
            zone='East',
            phase='1',
            cycle=1,
            child_10m=10,
        )

        self.assertEqual(len(mc), 1)
        self.assertEqual(mc['chainage_start'].iloc[0], 0)
        self.assertEqual(mc['chainage_end'].iloc[0], 10)
        self.assertIn('l1_start_lat', mc.columns)
        self.assertIn('r1_start_lat', mc.columns)
        self.assertIn('lhs_start_lat', mc.columns)
        self.assertIn('rhs_start_lat', mc.columns)
        self.assertEqual(mc['lanes_present'].iloc[0], 'L1,R1')

    def test_geometry_and_schema_enforcement(self):
        from src.hierarchy.geometry_builder import build_geometry_tables
        from config.table_schema import enforce_table_schema, FINAL_TABLE_SCHEMAS

        mc = pd.DataFrame({
            'chainage_start': [0],
            'chainage_end': [10],
            'upc_start_chainage': [0.0],
            'upc_end_chainage': [10.0],
            'l1_start_chainage': [0],
            'l1_end_chainage': [10],
            'l1_start_lat': [12.0],
            'l1_start_long': [77.0],
            'l1_end_lat': [12.0001],
            'l1_end_long': [77.0001],
            'nh_number': ['NH-27'],
            'section_code': ['SEC01'],
        })

        geom_p, geom_c = build_geometry_tables(mc, upc='UPC001')
        self.assertEqual(len(geom_p), 1)
        self.assertEqual(len(geom_c), 1)
        self.assertEqual(geom_p['lanes'].iloc[0], 'L1')
        self.assertEqual(geom_c['parent_id'].iloc[0], 'UPC001_geometry_parent')

        # Test schema enforcement on all 14 tables
        expected_counts = {
            'master_cracking_10m': 89,
            'master_cracking_as_per_ca': 89,
            'master_pothole': 88,
            'master_ravelling': 92,
            'master_ravelling_10m': 92,
            'master_roughness_as_per_ca': 93,
            'master_roughness_child_10m': 93,
            'master_roughness_parent': 93,
            'master_rutting_as_per_ca': 92,
            'master_rutting_child_10m': 92,
            'master_rutting_parent': 92,
            'geometry_parent': 22,
            'geometry_child': 13,
            'master_chainage': 54,
        }

        for tbl, exp_cols in expected_counts.items():
            self.assertIn(tbl, FINAL_TABLE_SCHEMAS)
            self.assertEqual(len(FINAL_TABLE_SCHEMAS[tbl]), exp_cols)

            dummy_df = pd.DataFrame([{'id': 'test_1', 'upc': 'UPC001'}])
            enforced = enforce_table_schema(dummy_df, tbl)
            self.assertEqual(len(enforced.columns), exp_cols)
            self.assertEqual(list(enforced.columns), FINAL_TABLE_SCHEMAS[tbl])


if __name__ == "__main__":
    unittest.main()
