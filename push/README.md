# push/ - Silver -> Postgres (dhms_uat), with the IDs populated from Postgres

* `silver_to_uat_push.py` - the push of `silver_to_uat_push_1.py` (github.com/shivamrajssai/silver_push_to_uat), same database,
  user, password and schema - taken from the machine (`config/db_config.py`: environment variables or the pipeline `.env`,
  names `DB_*` / `PG*` / `POSTGRES_*`). Nothing is stored in the repo. If the settings exist only inside the push script
  already on the machine: `python3 push/make_db_local_env.py <that script>` writes the git-ignored `config/db.local.env`.
  New flag `--populate-ids`: fills the project-level IDs from Postgres before the push and stops if one cannot be populated.
* `populate_ids_from_db.py` - the check / populate tool for a list of surveys (read-only DB session).
* The IDs: `project_id, piu_id, zone_id, state_id, region_id, contractor_id, ae_ie_id`. Value source: the UPC's row in
  `master_upc_project`, mapped through the DB foreign keys (a column referencing master_upc_project takes that column; a column
  referencing another master table takes the project column that references the same table), else the same-named column.
  Nothing is guessed: no source -> `UNMAPPED`. Why: the push sets a foreign-key value it cannot find in the DB to NULL.
* Order: 1) check  2) fix UNMAPPED / NO PROJECT  3) `--apply` (Silver rewritten, originals in `<silver>/_idpop_backup/<time>/`)
  4) push  5) `--check-db`.
* `push_surveys.py` - pushes a list of surveys one after the other (own transaction each, a failed survey does not stop the
  rest) with `--populate-ids`, and saves the log of every push plus `push_summary.csv` and `push_tables.csv` to
  `<silver root>/_push_logs/<time>/` in S3 (and `push/logs/push_<time>/`). Dry run unless `--dry-run false`.
