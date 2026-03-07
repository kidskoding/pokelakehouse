# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Transform Types & Abilities
# MAGIC Flatten and clean types/abilities data from Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# Load config
# TODO: Read from configs/pipeline_config.json
config = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Types

# COMMAND ----------

def transform_types(spark, config: dict):
    """
    Transform bronze types to silver:
    - Read from bronze_path/types
    - Flatten and clean
    - Extract: type_id, type_name, damage_relations
    - Add silver_processed_at
    - Write to silver_path/types as Delta
    """
    # TODO: Implement types transformation
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Abilities

# COMMAND ----------

def transform_abilities(spark, config: dict):
    """
    Transform bronze abilities to silver:
    - Read from bronze_path/abilities
    - Flatten and clean
    - Extract: ability_id, ability_name, is_hidden
    - Add silver_processed_at
    - Write to silver_path/abilities as Delta
    """
    # TODO: Implement abilities transformation
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Transforms

# COMMAND ----------

if __name__ == "__main__" or dbutils:
    # TODO: Call both transform functions
    # transform_types(spark, config)
    # transform_abilities(spark, config)
    pass
