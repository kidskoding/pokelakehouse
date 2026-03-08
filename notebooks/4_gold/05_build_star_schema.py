# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Build Star Schema
# MAGIC Fact and dimension tables for analytics

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Load config
# TODO: Read from configs/pipeline_config.json
config = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: fact_pokemon_stats

# COMMAND ----------

def build_fact_pokemon_stats(spark, config: dict):
    """
    Build fact_pokemon_stats:
    - pokemon_id, name
    - hp, attack, defense, special_attack, special_defense, speed
    - total_base_stats (computed: sum of all 6 stats)
    - type_1_id, type_2_id (foreign keys)

    Write to gold_path/fact_pokemon_stats as Delta
    """
    # TODO: Build fact table from silver pokemon
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_pokemon

# COMMAND ----------

def build_dim_pokemon(spark, config: dict):
    """
    Build dim_pokemon:
    - pokemon_id, name, height, weight, base_experience

    Write to gold_path/dim_pokemon as Delta
    """
    # TODO: Build pokemon dimension
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_type

# COMMAND ----------

def build_dim_type(spark, config: dict):
    """
    Build dim_type:
    - type_id, type_name

    Write to gold_path/dim_type as Delta
    """
    # TODO: Build type dimension
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_ability

# COMMAND ----------

def build_dim_ability(spark, config: dict):
    """
    Build dim_ability:
    - ability_id, ability_name, is_hidden

    Write to gold_path/dim_ability as Delta
    """
    # TODO: Build ability dimension
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Star Schema Build

# COMMAND ----------

if __name__ == "__main__" or dbutils:
    # TODO: Call all build functions
    # build_fact_pokemon_stats(spark, config)
    # build_dim_pokemon(spark, config)
    # build_dim_type(spark, config)
    # build_dim_ability(spark, config)
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Analytical Queries
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Top 10 Pokemon by total base stats
# MAGIC -- TODO: Write query
# MAGIC
# MAGIC -- 2. Average attack by type
# MAGIC -- TODO: Write query
# MAGIC
# MAGIC -- 3. Count of Pokemon per type
# MAGIC -- TODO: Write query
# MAGIC ```
