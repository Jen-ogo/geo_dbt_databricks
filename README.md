# geo_dbt_databricks

dbt project for the GEO pipeline (Databricks / Delta). Builds curated **SILVER** and **GOLD** layers from raw sources, with tests + documentation in YAML.

---

## Architecture
```bash
- **(RAW / SOURCES)**: external/raw ingested tables (Delta / external locations / Unity Catalog)
- **SILVER**: canonicalized / cleaned entities  
  (dedup, normalized types, consistent `region_code`, mandatory WKT debug fields, H3-ready attributes)
- **GOLD**: modeling / feature-ready marts  
  (H3 grids, aggregates, ML features; **prefer reusing SILVER aggregations**)
Macro vs Micro feature marts (GOLD)

To support scalable EV-siting and keep compute costs predictable, GOLD is split into two tiers:

MACRO (coarse grid, candidate discovery)
	•	Built on H3 R7 (and in some cases other coarse grids).
	•	Covers the full region grid (all cells from dim_h3_r7_cells).
	•	Purpose: generate candidate ranking / shortlist (where it makes sense to place EV charging).
	•	Inputs: SILVER entities + DIM cells (no dependency on micro marts).
MICRO (fine grid, deep scoring for shortlisted areas)
	•	Built on H3 R10 (fine resolution).
	•	Not computed for the whole country/region.
	•	Generated only for selected macro candidates (e.g., top-ranked R7 cells).
	•	Purpose: detailed scoring and local optimization inside shortlisted macro areas.
	•	Inputs: SILVER entities + dim_h3_r10_cells, filtered by candidate set.
Candidate-filtering rule (important)

Micro-level marts must be restricted to a candidate set produced by macro (example: top-N per region, or score threshold).
This prevents generating micro features for millions of cells unnecessarily.
---
```
## Requirements

- dbt (Cloud or Core) with **Databricks adapter**
- Access to workspace + catalog/schema:
  - `geo_databricks_sub.silver`
  - `geo_databricks_sub.gold`
- Delta Lake enabled (Unity Catalog recommended)
- Databricks SQL functions for:
  - spatial: `ST_*`
  - H3: `h3_*` (depending on runtime)

---

## Project conventions

### Materialization
- Databricks models are generally `materialized='table'` with:
  - `file_format='delta'`
  - `partition_by=[...]` (typically `region_code`, sometimes `region`)
- We keep models deterministic and re-buildable.

### Dedup
- SILVER entities are deduplicated using “latest wins” logic based on `load_ts`
  (pattern: window `row_number()` + filter `rn=1`).

### Geo debug is mandatory (where applicable)
- Row count checks
- WKT debug columns (e.g. `geom_wkt_4326`, `cell_wkt_4326`, `cell_center_wkt_4326`)
- Basic sanity checks (non-empty WKT, prefix checks like `POINT` / `POLYGON`)
- For geotables: always validate geometry fields and keep WKT for QA/inspection

### H3 conventions (critical)
- **Canonical H3 type across the project is STRING** (e.g. `8a1f...`)
- **Do not store BIGINT H3** in outputs.
- In GOLD: if an H3 aggregation already exists in SILVER, **reuse SILVER** instead of recomputing.

---

## Key macros / generic tests

Located in `macros/` and `tests/` (shared project utilities).  
Do **not** duplicate existing macros/tests — reuse them.

Common generic tests used across models:
- `rowcount_gt_0`
- `not_empty`
- `is_h3_hex`
- `non_negative`
- `wkt_not_empty`
- `wkt_prefix_any` / `wkt_prefix_in`
- `values_in_or_null`
- `num_between_or_null` (often WARN in GOLD for “business anomalies”)

---

## Running (dbt Cloud / CLI)

Build a single model:
```bash
dbt build -s <model_name>
```
Build all GOLD models:
```bash
dbt build -s gold.*
```

Run tests only:
```bash
dbt test -s <model_name or selector>
```