# Patches for UPCs that are already processed

## `patch_parent_breach.py` - parent and lane breach follows the "as per CA" children

The extraction now flags a 1000m record as a breach when its value is above the limit **or when any one
"as per CA" child in those 1000m is a breach** - the child table at the CA reporting interval, not the 10m rows. IRC is
not part of this rule. This patch applies it to the Silver parquets that are already in S3. The extraction is **not**
run again. It covers:

| Table | Children it looks at |
|---|---|
| `master_roughness_parent`, `master_rutting_parent` (LHS / RHS) | the rows of `master_roughness_as_per_ca` / `master_rutting_as_per_ca` of that **side** |
| `master_roughness_1000m_lane`, `master_rutting_1000m_lane`, `master_cracking_1000m_lane`, `master_ravelling_1000m_lane` (L1, L2, ... R1) | the rows of `master_<defect>_as_per_ca` (`master_ravelling` for ravelling) of that **lane** |

What changes on a parent / lane record that becomes a breach (a row that is already a breach never changes):

| Column | New value |
|---|---|
| `breach_flag` | 1 |
| `defect_id` | the `parent_id` |
| `road_condition` | `Bad` |
| `rating_ca` | the `Poor` band of the defect (the average could still rate Good / Very Good) - needs the row's `threshold_value`; if it is empty the rating is left and the report says so |
| `affected_length_ca`, `affected_length` | the parent's `total_length` (lanes: `length_m`) |
| `display_defect_id` | only if the row has none: a new `D/<ddmmyy>/N/<n>`, continuing the UPC's counter |
| `measures` (JSON) | `breach_ca`, `breach_from_child_ca`, `patched_parent_breach` |
| IRC columns | not touched |

**Defect IDs: if a breach row already has a `display_defect_id` it is kept; if it has none, one is generated**
(for parents and for lane records, including lane rows that were breaches before and never got an ID). The new
numbers continue the UPC's counter (highest counter of any of its tables + 1), so no ID is used twice. No existing
display ID of any table changes, so the children and the image upload log keep their IDs. Children that were
breaching without a flagged parent keep their own top-level ID.

```bash
cd /root/nsv/NHAI_NSV && git pull && source .venv/bin/activate

python3 patch/patch_parent_breach.py --all                     # 1. DRY RUN on every processed UPC: report only
python3 patch/patch_parent_breach.py --all --apply             # 2. patch them
```

Other selections (all of them also work as a dry run without `--apply`):

```bash
python3 patch/patch_parent_breach.py --zone B --phase 2 --cycle 1 --upc N_07024_01001_GJ --apply    # one UPC
python3 patch/patch_parent_breach.py --zone A --all --apply                                          # one zone (also --phase, --cycle)
python3 patch/patch_parent_breach.py --upc-file batches/upc_list1.txt --apply                        # a batch list (UPC ZONE PHASE CYCLE)
python3 patch/patch_parent_breach.py --all --silver-root s3://bucket/other/Silver                     # another silver root
```

* UPCs are found under `--silver-root` (default `$SILVER_S3_ROOT`, else `s3://dhms-new-stgae/DHMS/NSV/Silver`):
  `Zone=*/Phase=*/Cycle=*/UPC=*/dlm_kpi_new/`. `--workers N` runs N UPCs at the same time (default 4).
* **Nothing is written without `--apply`.** With `--apply`, every parquet that changes is first copied to
  `<silver-root>/_patch_backup/parent_breach_<time>/Zone=../Phase=../Cycle=../UPC=../dlm_kpi_new/` - copy it back to undo.
* Safe to run twice: a second run reports `unchanged`. A UPC without a parent or child table, or one that fails,
  is reported (`skipped` / `ERROR`) and never stops the others.
* A UPC processed **before the lane tables existed** has no `master_<defect>_1000m_lane` file: the patch reports it as
  `skipped` ("no lane table in this UPC"). The lane tables cannot be rebuilt from the saved parquets - run that UPC
  again with the extraction to get them (they then follow the rule from the start).
* Report per UPC and table (records, records turned into a breach, new IDs, status): `patch/logs/parent_breach_<time>.csv`
  and, with `--apply`, `<silver-root>/_patch_logs/parent_breach_<time>.csv`.
* Run the dry run first and look at the numbers; UPCs processed with the new extraction code already follow the rule
  and show `unchanged`.
