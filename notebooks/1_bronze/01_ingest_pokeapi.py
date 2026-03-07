# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest PokeAPI
# MAGIC Raw JSON ingestion from PokeAPI to Delta tables

# COMMAND ----------

import json
import requests
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

# COMMAND ----------

# Load config
# TODO: Read from configs/pipeline_config.json
config = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Pokemon

# COMMAND ----------

def fetch_pokemon_list(base_url: str, limit: int) -> list:
    """Fetch list of pokemon from /pokemon endpoint"""
    # TODO: Hit /pokemon?limit={limit} and return list of pokemon URLs
    pass

# COMMAND ----------

def fetch_pokemon_details(url: str) -> dict:
    """Fetch full pokemon details from individual endpoint"""
    # TODO: GET request to the pokemon detail URL, return JSON response
    pass

# COMMAND ----------

def ingest_pokemon(spark, config: dict):
    """
    Main ingestion for pokemon data.
    - Fetch all pokemon details
    - Convert to DataFrame with raw JSON
    - Add metadata: ingestion_timestamp, source_url
    - Write to bronze_path/pokemon as Delta
    """
    # TODO: Implement pokemon ingestion
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Types

# COMMAND ----------

def ingest_types(spark, config: dict):
    """
    Ingest /type endpoint to bronze_path/types
    - Add metadata columns
    - Write as Delta
    """
    # TODO: Implement type ingestion
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Abilities

# COMMAND ----------

def ingest_abilities(spark, config: dict):
    """
    Ingest /ability endpoint to bronze_path/abilities
    - Add metadata columns
    - Write as Delta
    """
    # TODO: Implement ability ingestion
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Ingestion

# COMMAND ----------

if __name__ == "__main__" or dbutils:
    # TODO: Call all ingestion functions
    # ingest_pokemon(spark, config)
    # ingest_types(spark, config)
    # ingest_abilities(spark, config)
    pass
