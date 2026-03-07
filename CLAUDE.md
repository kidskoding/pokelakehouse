# CLAUDE.md

## What This Project Is
A data engineering portfolio project built on Azure Databricks using PokeAPI as the data source.
Demonstrates production-grade DE patterns: medallion architecture, Delta Lake, pipeline orchestration, data quality gates.

## Architecture
```
PokeAPI (REST, no auth)
    ↓
[Bronze]  Raw JSON → Delta tables (land as-is, no transformation)
    ↓
[Silver]  Cleaned, typed, flattened DataFrames
    ↓
[Quality Gate]  Assertions — if this fails, Gold never runs
    ↓
[Gold]    Star schema — fact + dimension tables for analytics
    ↓
[Serving] SQL queries / Databricks Dashboard
```

## Repo Structure
```
pokelakehouse/
├── CLAUDE.md                        ← you are here
├── README.md
├── .gitignore
├── configs/
│   └── pipeline_config.json         ← all paths and params live here, no hardcoding
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
├── pipelines/
│   └── pokelakehouse_workflow.yml   ← Databricks Asset Bundle job definition
└── tests/
    └── test_transformations.py
```

## Tech Stack
- **Platform:** Azure Databricks (Free Edition or Azure-connected workspace)
- **Language:** Python / PySpark
- **Storage format:** Delta Lake exclusively (no raw Parquet or CSV)
- **Orchestration:** Databricks Workflows via Databricks Asset Bundles (DABs)
- **Source:** PokeAPI — https://pokeapi.co/api/v2 (free, no API key needed)
- **Tests:** pytest, mocking PokeAPI responses (no real HTTP calls in tests)

## Coding Conventions
- No hardcoded paths — always read from `configs/pipeline_config.json`
- All Delta writes use `.format("delta")` explicitly
- Notebooks are `.py` source format (not `.ipynb`) so they work in Databricks Repos
- Every notebook must be runnable standalone AND as a Workflow task
- Quality check notebook must `raise Exception` on failure — not just print a warning
- Add `ingestion_timestamp` / `processed_at` metadata columns at each layer

## Pipeline Task Order (enforced in workflow YAML)
1. `bronze_ingest` — hits PokeAPI, lands raw JSON as Delta
2. `silver_pokemon` — flattens pokemon (depends on bronze)
3. `silver_types_abilities` — flattens types + abilities (depends on bronze)
4. `quality_gate` — assertions on Silver data (depends on both silver tasks)
5. `gold_build` — star schema build (depends on quality_gate ONLY)

The quality gate is the critical dependency. Gold must never run if quality fails.

## Key Delta Lake Patterns to Use
- **Time Travel:** demonstrate with `VERSION AS OF` after an update
- **MERGE:** use for Silver upserts (not just overwrite)
- **Z-ORDER:** optimize Silver pokemon table by `pokemon_id`
- **Schema evolution:** use `mergeSchema` option when adding new columns
- **VACUUM:** include with comment explaining retention tradeoff

## Data Model

### Silver
- `silver_pokemon`: flattened stats (hp, attack, defense, sp_atk, sp_def, speed), type_1, type_2, ability_1/2/3
- `silver_types`: type_id, type_name, damage relations
- `silver_abilities`: ability_id, ability_name, is_hidden

### Gold (Star Schema)
- `fact_pokemon_stats`: pokemon_id, all 6 base stats, total_base_stats (computed), type_1_id, type_2_id
- `dim_pokemon`: pokemon_id, name, height, weight, base_experience
- `dim_type`: type_id, type_name
- `dim_ability`: ability_id, ability_name, is_hidden

## Common Commands
```bash
# Deploy pipeline to Databricks
databricks bundle deploy

# Run the full pipeline job
databricks bundle run pokelakehouse_job

# Run tests locally
pytest tests/

# Validate bundle config
databricks bundle validate
```

## What to Work On Next
When resuming, check which pipeline phase is incomplete:
1. Bronze notebooks done? → move to Silver
2. Silver done? → build quality gate
3. Quality gate done? → build Gold star schema
4. Gold done? → wire Databricks Workflow YAML and deploy with DABs
5. Deployed? → add Delta Lake features (time travel, MERGE, Z-ORDER)
6. All done? → write README and polish for portfolio/GitHub

## Git Conventions
Add co-author trailer to all commits: Co-authored-by: Claude <noreply@anthropic.com>