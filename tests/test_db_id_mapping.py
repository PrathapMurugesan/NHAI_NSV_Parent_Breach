import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.output.db_id_mapping import derive_sources, populate_df  # noqa: E402

PROJECT = {"id": "P-1", "upc": "N_01002_01001_GJ", "piu_id": "PIU-9", "nsv_zone_id": "Z-2", "state_id": "S-3",
           "region_id": "R-4", "contractor_id": None}


class TestMapping(unittest.TestCase):

    def test_sources(self):
        t_fks = [("project_id", "public", "master_upc_project", "id"), ("piu_id", "public", "master_piu", "id"),
                 ("parent_id", "public", "master_x_parent", "id")]
        p_fks = [("piu_id", "public", "master_piu", "id")]
        cols = ["project_id", "piu_id", "zone_id", "state_id", "contractor_id", "parent_id", "upc"]
        s = derive_sources(PROJECT, t_fks, p_fks, cols)
        self.assertEqual(s["project_id"]["value"], "P-1")
        self.assertEqual(s["piu_id"]["how"], "foreign key")
        self.assertEqual(s["zone_id"]["source"], "master_upc_project.nsv_zone_id")      # alias, no FK metadata
        self.assertEqual(s["state_id"]["value"], "S-3")
        self.assertIsNone(s["contractor_id"]["value"])                                # empty in the project row -> unmapped
        self.assertNotIn("parent_id", s)                                              # row-level FK is not a project ID
        self.assertNotIn("upc", s)

    def test_populate(self):
        df = pd.DataFrame({"id": ["a", "b", "c"], "project_id": [None, "", "OLD"], "piu_id": ["PIU-9"] * 3,
                           "contractor_id": [None] * 3})
        src = derive_sources(PROJECT, [], [], list(df.columns))
        out, st = populate_df(df, src)
        self.assertEqual(list(out["project_id"]), ["P-1"] * 3)
        by = {s["column"]: s for s in st}
        self.assertEqual((by["project_id"]["filled"], by["project_id"]["replaced"], by["project_id"]["status"]), (2, 1, "REPLACED"))
        self.assertEqual(by["piu_id"]["status"], "OK")
        self.assertEqual(by["contractor_id"]["status"], "UNMAPPED")
        self.assertEqual(by["contractor_id"]["blank_after"], 3)
        self.assertEqual(list(df["project_id"].fillna("")), ["", "", "OLD"])         # input untouched


if __name__ == "__main__":
    unittest.main()
