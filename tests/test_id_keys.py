import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.output.id_keys import apply_run_key, run_key  # noqa: E402

UPC = "N_07024_01001_GJ"


def tables():
    parent = pd.DataFrame({"id": [f"{UPC}_master_roughness_parent_LHS_0"], "parent_id": [f"{UPC}_LHS_0_1000"],
                           "defect_id": [f"{UPC}_LHS_0_1000"], "record_id": [f"{UPC}_LHS_0_1000"],
                           "source_defect_id": [f"{UPC}_LHS_0_1000"], "display_defect_id": ["D/110125/N/00001"],
                           "upc": ["N/07024/01001/GJ"], "upc_code": [UPC]})
    child = pd.DataFrame({"id": [f"{UPC}_master_roughness_as_per_ca_000001"], "parent_id": [f"{UPC}_LHS_0_1000"],
                          "defect_id": [f"D/110125/N/{UPC}/70/Roughness/0008"], "display_defect_id": ["D/110125/N/00001/0001"],
                          "upc_code": [UPC]})
    return {"master_roughness_parent": parent, "master_roughness_as_per_ca": child, "empty": pd.DataFrame()}


class TestRunKey(unittest.TestCase):

    def test_key(self):
        self.assertEqual(run_key(UPC, 1, 1), "N_07024_01001_GJ_P1_C1")

    def test_ids_carry_phase_and_cycle_and_links_hold(self):
        out, n = apply_run_key(tables(), UPC, 1, 2)
        k = f"{UPC}_P1_C2"
        p, c = out["master_roughness_parent"], out["master_roughness_as_per_ca"]
        self.assertEqual(p.loc[0, "id"], f"{k}_master_roughness_parent_LHS_0")
        self.assertEqual(c.loc[0, "id"], f"{k}_master_roughness_as_per_ca_000001")
        self.assertEqual(c.loc[0, "defect_id"], f"D/110125/N/{k}/70/Roughness/0008")
        self.assertEqual(c.loc[0, "parent_id"], p.loc[0, "defect_id"])            # child still points at its parent
        self.assertEqual(p.loc[0, "record_id"], p.loc[0, "source_defect_id"])
        self.assertEqual(p.loc[0, "display_defect_id"], "D/110125/N/00001")        # display ID untouched
        self.assertEqual((p.loc[0, "upc"], p.loc[0, "upc_code"]), ("N/07024/01001/GJ", UPC))   # upc columns untouched
        self.assertGreater(n["master_roughness_parent"], 0)

    def test_other_cycle_gives_other_ids_and_rerun_changes_nothing(self):
        a, _ = apply_run_key(tables(), UPC, 1, 1)
        b, _ = apply_run_key(tables(), UPC, 1, 2)
        self.assertNotEqual(a["master_roughness_parent"].loc[0, "id"], b["master_roughness_parent"].loc[0, "id"])
        again, n = apply_run_key(a, UPC, 1, 1)
        pd.testing.assert_frame_equal(a["master_roughness_parent"], again["master_roughness_parent"])
        self.assertEqual(sum(n.values()), 0)


if __name__ == "__main__":
    unittest.main()
