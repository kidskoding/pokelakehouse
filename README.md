# pokelakehouse

An end-to-end data engineering pipeline that ingests Pokémon data from the PokeAPI through a medallion architecture:
- **Bronze** for raw ingestion
- **Silver** for cleaning and transformation
- **Gold** for a star schema analytical layer

Built on **Azure Databricks** using Databricks Asset Bundles, Databricks Workflows, PySpark, and Delta Lake, with a data quality gate that blocks the Gold layer if Silver assertions fail.

## Running in Databricks

> This is designed to run in **Databricks** workspace

### Setup
1. Create a Databricks workspace (Azure Databricks or Community Edition)
2. Go to **Repos** in the sidebar
3. Click **Add Repo** and paste this repository URL
4. Databricks will clone the repo into your workspace

### Running Notebooks Manually
1. Navigate to any notebook under `notebooks/`
2. Attach to a cluster (Runtime 13.0+ with Python and PySpark)
3. Click **Run All** or run cells individually

Run notebooks in order:
1. `1_bronze/01_ingest_pokeapi.py` - Ingest raw data from PokeAPI
2. `2_silver/02_transform_pokemon.py` - Clean and flatten pokemon data
3. `2_silver/03_transform_types_abilities.py` - Clean types and abilities
4. `3_quality/05_data_quality_checks.py` - Run quality assertions
5. `4_gold/04_build_star_schema.py` - Build star schema for analytics

### Running via Databricks Workflows
Deploy and run the full pipeline using Databricks Asset Bundles:
```bash
databricks bundle deploy
databricks bundle run pokelakehouse_job
```