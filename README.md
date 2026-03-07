# pokelakehouse

An end-to-end data engineering pipeline that ingests Pokémon data from the PokeAPI through a medallion architecture:
- **Bronze** for raw ingestion
- **Silver** for cleaning and transformation
- **Gold** for a star schema analytical layer

Built on **Azure Databricks** using Databricks Asset Bundles, Databricks Workflows, PySpark, and Delta Lake, with a data quality gate that blocks the Gold layer if Silver assertions fail.