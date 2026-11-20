# DLM Extraction Pipeline (AWS EC2 Modular Edition)

A modular Python application converted from the Databricks DLM Extraction Pipeline notebook (`DLM Extraction Pipeline Latest (Updated_Nov_18).ipynb`). Designed for high-performance, standalone execution on AWS EC2 or local Linux environments.

---

## 1. System Architecture

```
dlm-extraction-pipeline/
│
├── main.py                     # CLI and batch orchestration entrypoint
├── config/
│   ├── settings.py             # PipelineSettings, .env/CLI resolution, partition discovery
│   └── table_schema.py         # Table schemas, Silver table specs, workbook schema manager
│
├── src/
│   ├── common/
│   │   ├── logger.py           # Thread-safe logging to console and rotating log file
│   │   ├── exceptions.py       # Custom pipeline error hierarchy
│   │   └── constants.py        # Business constants, weights, lengths, normalizers
│   │
│   ├── ingestion/
│   │   ├── tracker_loader.py   # Tracker Excel ingestion, date/interval extraction
│   │   ├── bronze_loader.py    # Raw defect consolidation & condition categorizers
│   │   ├── backup_loader.py    # Lane=* parquet discovery & loading
│   │   ├── d1_loader.py        # D1 LRMS & section context lookup
│   │   ├── d2_loader.py        # D2 CA & IRC thresholds parsing
│   │   ├── d3_loader.py        # D3 condition sheet loader
│   │   └── d4_loader.py        # D4 pothole loader & chainage normalization
│   │
│   ├── processing/
│   │   ├── pothole.py          # Pothole processing & D2 fallback loader
│   │   ├── cracking.py         # Crack calibration (1m cap) & severity categorization
│   │   ├── roughness.py        # IRI/BI linear set generation (10m, 100m, 1km)
│   │   ├── rutting.py          # Rut depth linear set generation (10m, 100m, 1km)
│   │   ├── ravelling.py        # Ravelling % area linear set generation (10m, 100m, 1km)
│   │   ├── pci.py              # Max-observed normalization & PCI weighted rollup
│   │   ├── road_condition.py   # Dynamic D3 & CA Good/Bad condition assignment
│   │   └── ratings.py          # NH number standardization & independent CA/IRC ratings
│   │
│   ├── hierarchy/
│   │   ├── parent_builder.py   # LHS/RHS 1000m side parents (roughness, rutting)
│   │   ├── child_builder.py    # Child builders: child_ca, child_10m, child_irc100
│   │   ├── lhs_rhs_builder.py  # Hierarchy coordinator & display defect ID sequencing
│   │   ├── geometry_builder.py # geometry_parent & geometry_child generator
│   │   └── chainage_builder.py # master_chainage generation with per-lane/side GPS
│   │
│   ├── output/
│   │   ├── table_writer.py     # Parquet writer with replaceWhere UPC/Phase/Cycle semantics
│   │   ├── image_uploader.py   # Multi-threaded CDN upload for breach images
│   │   ├── excel_exporter.py   # Consolidated multi-sheet Excel workbook exporter
│   │   └── csv_exporter.py     # CSV exporter with _index.csv integrity report
│   │
│   └── validation/
│       ├── validator.py        # Comprehensive V1-V9 validation checks
│       ├── validation_report.py# Formatted console validation summary
│       └── batch_report.py     # Multi-UPC validation pivot and detail sheets
│
├── scripts/
│   ├── run_upc.sh              # Wrapper script for single UPC
│   └── run_batch.sh            # Wrapper script for batch processing
│
├── batches/
│   └── upcs_4.txt              # Sample batch UPC list
│
├── tests/
│   ├── test_ingestion.py       # Unit tests for loaders & normalizers
│   ├── test_processing.py      # Unit tests for defect formulas & PCI
│   ├── test_hierarchy.py       # Unit tests for LHS/RHS hierarchy & display IDs
│   └── test_validation.py     # Unit tests for V1-V9 validator checks
│
├── requirements.txt            # Python dependencies
├── .env                        # Environment variable configuration template
└── README.md                   # System documentation
```

---

## 2. Key Business Formulas & Logic Preserved

1. **Roughness Bump Integrator (BI):**
   $$\text{BI} = 630 \times \text{IRI}^{1.12}$$
   Computed from LWP and RWP IRI averages at 100m and 1000m intervals.

2. **Cracking Calibration:**
   Backup parquet raw sums per 10m are calibrated against D2 1m-capped values ($3.5\,\text{m}^2$ max per 1m bin).

3. **Cracking Corrected Width Severity:**
   - Width $< 3\,\text{mm} \implies \text{None}$
   - $3\,\text{mm} \le \text{Width} < 6\,\text{mm} \implies \text{Medium}$
   - $\text{Width} \ge 6\,\text{mm} \implies \text{High}$

4. **Ravelling Percentage:**
   $$\text{Ravelling \%} = \frac{\text{Ravelling Area}}{3.5 \times \text{Length}} \times 100$$

5. **Pavement Condition Index (PCI):**
   $$\text{PCI} = 100 - \text{Weighted Defect Score}$$
   - Weights: $\text{Crack}=0.12$, $\text{Ravel}=0.10$, $\text{Pothole}=0.16$, $\text{Patch}=0.08$, $\text{Rut}=0.14$, $\text{Rough}=0.40$
   - Each component is scaled 0–100 using max-observed value normalization across the survey.

6. **LHS / RHS Silver Hierarchy:**
   - **Roughness & Rutting:** 1000m parents aggregated by side (`LHS` for L-lanes, `RHS` for R-lanes), one record per side per km.
     All 10m 'As per CA' rows of the side's lanes are pooled:
     Roughness: Side IRI = (mean LWP + mean RWP)/2, Side BI = 630 × IRI^1.12. Rutting: Side rut = (mean Left + mean Right)/2.
     `breach_flag` = side value ≥ CA limit; `affected_length_irc` / `rating_irc` = side value ≥ IRC limit on the same record (no IRC rows in the parent).
   - **As per CA tables:** roughness, rutting and cracking are aggregated at the tracker's CA reporting interval (lane-wise). Ravelling is fixed at 100m.
   - **IRC limits:** taken from D2 (defaults when missing) and applied with ≥; IRC rows exist only in the 10m tables.
   - **Cracking & Ravelling:** Top tables are lane-wise (`master_cracking_as_per_ca`, `master_ravelling`) with `parent_id = NULL`. Child 10m rows link directly to top table rows.
   - **Display Defect IDs:** Formatted as `D/{ddmmyy}/N/{counter:05d}` for top breaches, `{parent_display_id}/{counter:04d}` for child breaches, and orphan breaches receive new top counter sequences.

---

## 3. EC2 Setup & Execution

### Prerequisites
- Python 3.9+
- Linux (Ubuntu 20.04/22.04 or Amazon Linux 2023)

### Installation
```bash
git clone <repo_url>
cd dlm-extraction-pipeline
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Configuration
Copy or edit `.env`:
```bash
cp .env.example .env   # or edit .env directly
```

### Running a Single UPC
```bash
./scripts/run_upc.sh East 1 1 20240900000000001 /path/to/data_root
# OR via python CLI:
python3 main.py --zone East --phase 1 --cycle 1 --upc 20240900000000001 --data-root /path/to/data_root
```

### Running a Batch
```bash
./scripts/run_batch.sh East 1 1 batches/upcs_4.txt /path/to/data_root
# OR via python CLI:
python3 main.py --zone East --phase 1 --cycle 1 --batch-file batches/upcs_4.txt --data-root /path/to/data_root
```

### Running the Test Suite
```bash
python3 -m unittest discover tests
```

---

## 4. Final Tables & Output Schemas

| # | Table Name | Columns | Level | Description |
|---|---|:---:|---|---|
| 1 | `master_cracking_10m` | 89 | `child_10m` | Lane-wise 10m cracking (CA + IRC rows) |
| 2 | `master_cracking_as_per_ca` | 89 | `as_per_ca` | Lane-wise CA reporting interval cracking |
| 3 | `master_pothole` | 88 | `parent` | Defect records (1 row per pothole) |
| 4 | `master_ravelling` | 92 | `master` | Lane-wise 100m constant CA ravelling |
| 5 | `master_ravelling_10m` | 92 | `child_10m` | Lane-wise 10m ravelling (CA + IRC rows) |
| 6 | `master_roughness_as_per_ca` | 93 | `child_as_per_ca` | Lane-wise CA reporting interval roughness |
| 7 | `master_roughness_child_10m` | 93 | `child_10m` | Lane-wise 10m roughness (CA + IRC rows) |
| 8 | `master_roughness_parent` | 93 | `parent` | LHS / RHS 1000m parent roughness |
| 9 | `master_rutting_as_per_ca` | 92 | `child_as_per_ca` | Lane-wise CA reporting interval rutting |
| 10 | `master_rutting_child_10m` | 92 | `child_10m` | Lane-wise 10m rutting (CA + IRC rows) |
| 11 | `master_rutting_parent` | 92 | `parent` | LHS / RHS 1000m parent rutting |
| 12 | `geometry_parent` | 22 | `parent` | UPC-level geometry summary & surveyed length |
| 13 | `geometry_child` | 13 | `child` | Surveyed geometry segments per lane with GPS |
| 14 | `master_chainage` | 54 | `master` | Continuous 10m chainage spine with lane & side GPS |
| 15 | `dlm_validation_results` | 9 | `report` | Unified V1–V9 validation checks report |
| 16 | `master_roughness_1000m_lane` | 93 | `lane_1000m` | Lane-wise (L1, L2, ... R1, ...) 1000m roughness, not pooled into LHS / RHS |
| 17 | `master_rutting_1000m_lane` | 92 | `lane_1000m` | Lane-wise 1000m rutting |
| 18 | `master_cracking_1000m_lane` | 89 | `lane_1000m` | Lane-wise 1000m cracking |
| 19 | `master_ravelling_1000m_lane` | 92 | `lane_1000m` | Lane-wise 1000m ravelling |

The `master_<defect>_1000m_lane` tables (no pothole) have one record per lane per 1000m, from the lane-wise 1000m aggregate
(CA values). `breach_flag` / `affected_length_ca` are set when the lane's value is at or above the CA threshold **or any one 10m
child of that lane in those 1000m breaches**; `affected_length_irc` (and `rating_irc` for roughness and rutting) use the IRC limit
or an IRC 10m child breach, on the same record. The LHS / RHS parents follow the same rule per side. Processed UPCs can be brought
up to date with `patch/patch_parent_breach.py` (see `patch/README.md`). Breach rows of the lane tables get a
`display_defect_id` continuing the UPC's counter (a row that already has one keeps it). The lane tables are numbered after the main
tables, so the IDs of the other tables (the LHS / RHS parents and the children) do not change.

