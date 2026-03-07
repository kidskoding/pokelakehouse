# pokelakehouse

An end-to-end data engineering pipeline that ingests Pokémon data from the PokeAPI through a medallion architecture:
- **Bronze** for raw ingestion
- **Silver** for cleaning and transformation
- **Gold** for a star schema analytical layer

Built on **Azure Databricks** using Databricks Asset Bundles, Databricks Workflows, PySpark, and Delta Lake, with a data quality gate that blocks the Gold layer if Silver assertions fail.

## Running in Databricks

### Setup
1. Create a Databricks workspace (Azure Databricks or Community Edition)
2. Go to **Repos** in the sidebar
3. Click **Add Repo** and paste this repository URL
4. Databricks will clone the repo into your workspace

### Running Notebooks
1. Navigate to `notebooks/1_bronze/01_ingest_pokeapi.py`
2. Attach to a cluster (any cluster with Python and PySpark)
3. Click **Run All** or run cells individually

The notebook will:
- Fetch 151 Pokémon from PokeAPI
- Fetch all types and abilities
- Write raw JSON to Delta tables at `dbfs:/pokelakehouse/bronze/`