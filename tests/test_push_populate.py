"""run_survey of push/populate_ids_from_db.py with a fake DB cursor and a local parquet (no network)."""
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "push"))
import populate_ids_from_db as PID  # noqa: E402


class FakeCur:
    def __init__(self, rows, names):
        self._rows, self.description = rows, [(n,) for n in names]

    def execute(self, *a, **k):
        pass

    def fetchall(self):
        return self._rows


class TestRunSurvey(unittest.TestCase):

    def test_check_then_apply(self):
        tmp = tempfile.mkdtemp()
        f = os.path.join(tmp, "master_rutting_parent.parquet")
        pd.DataFrame({"id": ["a", "b"], "project_id": [None, ""], "piu_id": [None, "PIU-9"], "zone_id": ["OLD", "OLD"],
                      "contractor_id": [None, None]}).to_parquet(f, index=False)
        cols = {c: {} for c in ("id", "project_id", "piu_id", "zone_id", "contractor_id", "state_id")}
        push = SimpleNamespace(
            DB_SCHEMA="public", parse_upc_variations=lambda u: [u], resolve_zone_from_db=lambda c, u: "B",
            locate_s3_parquet_files=lambda *a, **k: {"master_rutting_parent.parquet": f},
            foreign_keys=lambda c, s, t: [("piu_id", "public", "master_piu", "id")] if t == "master_rutting_parent" else
            [("piu_id", "public", "master_piu", "id")],
            SILVER_TABLE_SPECS=[("master_rutting_parent.parquet", "master_rutting_parent", [])],
            db_table_exists=lambda c, s, t: True, db_columns=lambda c, s, t: cols)
        cur = FakeCur([("P-1", "U1", "PIU-9", "Z-2", None)], ["id", "upc", "piu_id", "nsv_zone_id", "contractor_id"])
        rows = PID.run_survey(push, cur, None, "U1", "1", "1", None, "s3://x/Silver")
        by = {r["column"]: r for r in rows if r.get("column") in cols}
        self.assertEqual(by["project_id"]["status"], "FILLED")
        self.assertEqual(by["piu_id"]["status"], "FILLED")
        self.assertEqual(by["zone_id"]["status"], "REPLACED")
        self.assertEqual(by["contractor_id"]["status"], "UNMAPPED")           # empty in the project row: reported, not guessed
        self.assertTrue(any(r["status"] == "NOT IN SILVER" for r in rows))   # state_id: DB column absent from Silver
        self.assertEqual(list(pd.read_parquet(f)["project_id"].fillna("")), ["", ""])   # check mode writes nothing

        PID.run_survey(push, cur, None, "U1", "1", "1", None, "s3://x/Silver", apply=True, backup_root=os.path.join(tmp, "bk"))
        out = pd.read_parquet(f)
        self.assertEqual(list(out["project_id"]), ["P-1", "P-1"])
        self.assertEqual(list(out["piu_id"]), ["PIU-9", "PIU-9"])
        self.assertEqual(list(out["zone_id"]), ["Z-2", "Z-2"])
        self.assertTrue(os.path.exists(os.path.join(tmp, "bk", "master_rutting_parent.parquet")))   # original kept first

    def test_no_project_row(self):
        push = SimpleNamespace(DB_SCHEMA="public", parse_upc_variations=lambda u: [u])
        rows = PID.run_survey(push, FakeCur([], ["id"]), None, "U1", "1", "1", None, "s3://x")
        self.assertEqual(rows[0]["status"], "NO PROJECT")


if __name__ == "__main__":
    unittest.main()
