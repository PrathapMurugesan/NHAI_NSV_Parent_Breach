"""push/push_surveys.py with a stub push script: logs and reports of every survey are kept, also for failures."""
import os
import sys
import tempfile
import textwrap
import unittest

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "push"))
import push_surveys as PS  # noqa: E402

STUB = textwrap.dedent('''
    import sys
    a = sys.argv
    upc = a[a.index("--upc") + 1]
    live = a[a.index("--dry-run") + 1] == "false"
    print("SUMMARY:")
    if upc == "BAD":
        print("  !! 3 IDs cannot be populated from Postgres")
        print("Push aborted: some IDs cannot be populated from Postgres. DB unchanged.")
        sys.exit(1)
    if live:
        print("  master_x                    : UPDATED 20 rows, INSERTED 1,000 rows")
    else:
        print("  master_x                    : would INSERT 1,000 rows, UPDATE 20 rows (5 with changed cells)")
''')


class TestPushSurveys(unittest.TestCase):

    def _run(self, dry):
        tmp = tempfile.mkdtemp()
        stub, lst, logs = (os.path.join(tmp, n) for n in ("stub.py", "s.txt", "logs"))
        open(stub, "w").write(STUB)
        open(lst, "w").write("upc\tcycle\tphase\nN/01002/01001/GJ\t1\t1\nBAD\t2\t1\nN/08081/01002/GJ\t2\t1\n")
        rc = PS.main(["--input-file", lst, "--zone", "B", "--dry-run", dry, "--push-script", stub,
                      "--s3-root", tmp, "--log-root", logs])
        run = os.path.join(logs, os.listdir(logs)[0])
        return rc, run

    def test_live_reports_and_failure_does_not_stop_the_rest(self):
        rc, run = self._run("false")
        self.assertEqual(rc, 1)                                              # one survey failed
        s = pd.read_csv(os.path.join(run, "push_summary.csv"))
        self.assertEqual(list(s["status"]), ["SUCCESS", "FAILED", "SUCCESS"])   # the third still ran
        self.assertEqual((int(s.loc[0, "rows_inserted"]), int(s.loc[0, "rows_updated"])), (1000, 20))
        self.assertIn("cannot be populated", s.loc[1, "error"])
        t = pd.read_csv(os.path.join(run, "push_tables.csv"))
        self.assertEqual(len(t), 2)                                          # the failed survey has no table rows
        logs = sorted(n for n in os.listdir(run) if n.endswith(".log"))
        self.assertEqual(logs, ["BAD_C2_P1.log", "N_01002_01001_GJ_C1_P1.log", "N_08081_01002_GJ_C2_P1.log"])
        self.assertIn("Push aborted", open(os.path.join(run, "BAD_C2_P1.log")).read())

    def test_dry_run_is_labelled(self):
        rc, run = self._run("true")
        s = pd.read_csv(os.path.join(run, "push_summary.csv"))
        self.assertEqual(list(s["status"]), ["DRY RUN OK", "DRY RUN FAILED", "DRY RUN OK"])
        self.assertEqual(int(s.loc[0, "rows_inserted"]), 1000)


if __name__ == "__main__":
    unittest.main()
