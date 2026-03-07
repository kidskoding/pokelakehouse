# PokéLakehouse — Project Spec for Claude Code

## What to Build
Scaffold a complete Azure Databricks data engineering portfolio project called `pokelakehouse`.
The data source is the **PokeAPI** (https://pokeapi.co) — free, no auth required.
The architecture follows the **medallion pattern**: Bronze → Silver → Gold, orchestrated as a real pipeline.

---

## Repo Structure to Generate

```
pokelakehouse/
│
├── README.md
├── .gitignore
│
├── configs/
│   └── pipeline_config.json
│
├── notebooks/
│   ├── bronze/
│   │   └── 01_ingest_pokeapi.py
│   ├── silver/
│   │   ├── 02_transform_pokemon.py
│   │   └── 03_transform_types_abilities.py
│   ├── gold/
│   │   └── 04_build_star_schema.py
│   └── quality/
│       └── 05_data_quality_checks.py
│
├── pipelines/
│   └── pokelakehouse_workflow.yml
│
└── tests/
    └── test_transformations.py
```

---

## File Specifications

### `.gitignore`
Exclude: `__pycache__/`, `.env`, `*.pyc`, `.databricks/`, `dist/`, `bundle.yml.lock`

---

### `configs/pipeline_config.json`
```json
{
  "api": {
    "base_url": "https://pokeapi.co/api/v2",
    "endpoints": ["pokemon", "type", "ability", "move"],
    "limit": 151
  },
  "storage": {
    "bronze_path": "dbfs:/pokelakehouse/bronze",
    "silver_path": "dbfs:/pokelakehouse/silver",
    "gold_path": "dbfs:/pokelakehouse/gold"
  },
  "pipeline": {
    "batch_size": 50,
    "enable_quality_gate": true
  }
}
```

---

### `notebooks/bronze/01_ingest_pokeapi.py`
- Read config from `pipeline_config.json`
- Hit PokeAPI `/pokemon?limit=151` to get the first 151 Pokemon
- For each Pokemon, fetch the full detail endpoint `/pokemon/{id}`
- Store raw JSON responses as a Delta table at `bronze_path/pokemon`
- Add metadata columns: `ingestion_timestamp`, `source_url`
- Also ingest `/type` and `/ability` endpoints into their own Bronze Delta tables
- Use `requests` for API calls, convert to Spark DataFrame, write as Delta

---

### `notebooks/silver/02_transform_pokemon.py`
- Read from `bronze/pokemon` Delta table
- Flatten nested fields:
  - `stats` array → individual columns: `hp`, `attack`, `defense`, `special_attack`, `special_defense`, `speed`
  - `types` array → `type_1`, `type_2` (nullable)
  - `abilities` array → `ability_1`, `ability_2`, `ability_3` (nullable)
- Cast all types correctly (ints, strings, nulls)
- Deduplicate on `pokemon_id`
- Write to `silver_path/pokemon` as Delta with `overwrite` mode
- Add `silver_processed_at` timestamp column

---

### `notebooks/silver/03_transform_types_abilities.py`
- Read from `bronze/type` and `bronze/ability` Delta tables
- Flatten and clean both
- Write to `silver_path/types` and `silver_path/abilities`

---

### `notebooks/gold/04_build_star_schema.py`
Fact table: `fact_pokemon_stats`
- Columns: `pokemon_id`, `name`, `hp`, `attack`, `defense`, `special_attack`, `special_defense`, `speed`, `total_base_stats`, `type_1_id`, `type_2_id`
- `total_base_stats` = sum of all 6 base stats (computed column)

Dimension tables:
- `dim_pokemon`: `pokemon_id`, `name`, `height`, `weight`, `base_experience`
- `dim_type`: `type_id`, `type_name`
- `dim_ability`: `ability_id`, `ability_name`, `is_hidden`

Write all to `gold_path/` as Delta tables.

Also write 3 example analytical SQL queries as comments at the bottom:
1. Top 10 Pokemon by total base stats
2. Average attack by type
3. Count of Pokemon per type

---

### `notebooks/quality/05_data_quality_checks.py`
Run these checks and raise an exception if any fail (this halts the pipeline):
- `silver/pokemon` has > 100 rows
- No nulls on `pokemon_id` or `name`
- `hp`, `attack`, `defense` are all > 0
- `type_1` is never null
- Row count of Silver >= row count of Bronze (no data loss)
- Print a quality report summary at the end

---

### `pipelines/pokelakehouse_workflow.yml`
Databricks Asset Bundle job definition with these tasks in order:
1. `bronze_ingest` → runs `notebooks/bronze/01_ingest_pokeapi.py`
2. `silver_pokemon` → runs `notebooks/silver/02_transform_pokemon.py`, depends on `bronze_ingest`
3. `silver_types_abilities` → runs `notebooks/silver/03_transform_types_abilities.py`, depends on `bronze_ingest`
4. `quality_gate` → runs `notebooks/quality/05_data_quality_checks.py`, depends on `silver_pokemon` and `silver_types_abilities`
5. `gold_build` → runs `notebooks/gold/04_build_star_schema.py`, depends on `quality_gate`

The quality gate MUST be between Silver and Gold — if it fails, Gold never runs.

---

### `tests/test_transformations.py`
Unit tests (pytest) for pure transformation logic:
- Test that the stats flattening function produces correct column names
- Test that `total_base_stats` sums correctly
- Test that null handling on `type_2` works
- Mock the PokeAPI response so no real HTTP calls are made

---

### `README.md`
Include:
- Project overview (medallion architecture on Azure Databricks with PokeAPI)
- Architecture diagram in ASCII
- Setup instructions: clone repo, install Databricks CLI, configure workspace, run `databricks bundle deploy`
- Pipeline stages explained
- How to connect GitHub repo to Databricks Repos
- Tech stack: PySpark, Delta Lake, Databricks Workflows, Databricks Asset Bundles, Python

---

## Key Principles to Follow
- No hardcoded paths — everything reads from `pipeline_config.json`
- Every notebook should be runnable standalone AND as part of the Workflow
- The quality check notebook must raise an exception on failure (not just print a warning)
- All Delta writes use `.format("delta")` explicitly
- Notebooks are `.py` files (Databricks source format), not `.ipynb`