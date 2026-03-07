# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Transform Pokemon
# MAGIC Flatten and clean pokemon data from Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from datetime import datetime

# COMMAND ----------

# Load config
# TODO: Read from configs/pipeline_config.json
config = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

def read_bronze_pokemon(spark, bronze_path: str):
    """Read raw pokemon data from bronze layer"""
    # TODO: Read Delta table from bronze_path/pokemon
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten Stats

# COMMAND ----------

def flatten_stats(df):
    """
    Flatten stats array into individual columns:
    - hp, attack, defense, special_attack, special_defense, speed
    """
    # TODO: Extract each stat from the stats array
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten Types

# COMMAND ----------

def flatten_types(df):
    """
    Flatten types array into:
    - type_1 (required)
    - type_2 (nullable)
    """
    # TODO: Extract type_1 and type_2 from types array
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten Abilities

# COMMAND ----------

def flatten_abilities(df):
    """
    Flatten abilities array into:
    - ability_1, ability_2, ability_3 (all nullable except ability_1)
    """
    # TODO: Extract abilities from abilities array
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Transform

# COMMAND ----------

def transform_pokemon(spark, config: dict):
    """
    Main transformation pipeline:
    1. Read bronze pokemon
    2. Flatten stats, types, abilities
    3. Cast types correctly
    4. Deduplicate on pokemon_id
    5. Add silver_processed_at timestamp
    6. Write to silver_path/pokemon as Delta (overwrite)
    """
    # TODO: Implement full transformation pipeline
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Transform

# COMMAND ----------

if __name__ == "__main__" or dbutils:
    # TODO: Call transform_pokemon(spark, config)
    pass
