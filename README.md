# PokeLakehouse

A data engineering portfolio project demonstrating production-grade patterns using Pokemon data.

## Architecture

```
PokeAPI (REST)
     |
     v
+----------+     +----------+     +--------------+     +----------+
|  Bronze  | --> |  Silver  | --> | Quality Gate | --> |   Gold   |
|  (raw)   |     | (cleaned)|     | (assertions) |     |  (star)  |
+----------+     +----------+     +--------------+     +----------+
                                         |
                                    Fails? Stop.
                                         |
                                         v
                              Analytics View (Genie/Dashboards)
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Platform | Azure Databricks |
| Storage | Delta Lake |
| Processing | PySpark |
| Orchestration | Databricks Workflows |
| Deployment | Databricks Asset Bundles |
| CI/CD | GitHub Actions |
| Data Source | [PokeAPI](https://pokeapi.co/) |

## Project Structure

```
pokelakehouse/
├── notebooks/
│   ├── 1_bronze/
│   │   └── 01_ingest_pokeapi.py      # Fetch raw JSON from API
│   ├── 2_silver/
│   │   ├── 02_transform_pokemon.py   # Flatten pokemon data
│   │   └── 03_transform_types_abilities.py
│   ├── 3_quality/
│   │   └── 04_data_quality_checks.py # Assertions before gold
│   └── 4_gold/
│       └── 05_build_star_schema.py   # Fact + dimension tables
├── pipelines/
│   └── pokelakehouse_workflow.yml    # Databricks job definition
├── .github/workflows/
│   └── deploy.yml                    # Auto-deploy on push
├── databricks.yml                    # Bundle config
└── constants.py                      # Shared table names
```

## Data Model

### Silver Layer (Cleaned)
- `silver.pokemon` - Flattened stats, types, abilities
- `silver.types` - Type names and damage relations
- `silver.abilities` - Ability names and effects

### Gold Layer (Star Schema)
- `fact_pokemon_stats` - Stats + FK to dimensions
- `dim_pokemon` - Pokemon attributes
- `dim_type` - Type lookup
- `dim_ability` - Ability lookup
- `v_pokemon_analytics` - Pre-joined view for dashboards

## Pipeline Flow

```
bronze_ingest
      |
      v
+-----+-----+
|           |
v           v
silver_pokemon    silver_types_abilities
|           |
+-----+-----+
      |
      v
quality_gate  <-- Fails = pipeline stops
      |
      v
gold_build
```

## Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed

### Deploy & Run
```bash
# Configure CLI
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=your-token

# Deploy
databricks bundle deploy

# Run pipeline
databricks bundle run pokelakehouse_job
```

### CI/CD
Push to `master` triggers auto-deploy via GitHub Actions.

Required secrets in GitHub:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

## Key Patterns Demonstrated

- **Medallion Architecture** - Bronze/Silver/Gold layers
- **Delta Lake** - ACID transactions, schema enforcement
- **Quality Gates** - Assertions block downstream if data is bad
- **Star Schema** - Fact + dimension tables for analytics
- **CI/CD** - GitHub Actions auto-deploy
- **Asset Bundles** - Infrastructure as code for Databricks
