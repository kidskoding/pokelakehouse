# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Build Star Schema
# MAGIC Fact and dimension tables for analytics

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

from constants import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Gold Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_type

# COMMAND ----------

def build_dim_type(spark):
    """
    Build dim_type:
    - type_id (surrogate key)
    - type_name
    """
    df = spark.table(SILVER_TYPES)

    dim = df.select(
        F.col("type_id"),
        F.col("type_name"),
        F.lit(datetime.utcnow().isoformat()).alias("loaded_at")
    )

    dim.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIM_TYPE)
    print(f"Built {GOLD_DIM_TYPE} with {dim.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_ability

# COMMAND ----------

def build_dim_ability(spark):
    """
    Build dim_ability:
    - ability_id
    - ability_name
    - is_main_series
    """
    df = spark.table(SILVER_ABILITIES)

    dim = df.select(
        F.col("ability_id"),
        F.col("ability_name"),
        F.col("is_main_series"),
        F.lit(datetime.utcnow().isoformat()).alias("loaded_at")
    )

    dim.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIM_ABILITY)
    print(f"Built {GOLD_DIM_ABILITY} with {dim.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: dim_pokemon

# COMMAND ----------

def build_dim_pokemon(spark):
    """
    Build dim_pokemon:
    - pokemon_id
    - name
    - height
    - weight
    - base_experience
    """
    df = spark.table(SILVER_POKEMON)

    dim = df.select(
        F.col("pokemon_id"),
        F.col("name"),
        F.col("height"),
        F.col("weight"),
        F.col("base_experience"),
        F.lit(datetime.utcnow().isoformat()).alias("loaded_at")
    )

    dim.write.format("delta").mode("overwrite").saveAsTable(GOLD_DIM_POKEMON)
    print(f"Built {GOLD_DIM_POKEMON} with {dim.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: fact_pokemon_stats

# COMMAND ----------

def build_fact_pokemon_stats(spark):
    """
    Build fact_pokemon_stats:
    - pokemon_id (FK to dim_pokemon)
    - hp, attack, defense, sp_atk, sp_def, speed
    - total_base_stats (computed: sum of all 6 stats)
    - type_1_id, type_2_id (FK to dim_type)
    """
    pokemon = spark.table(SILVER_POKEMON)
    types = spark.table(GOLD_DIM_TYPE)

    # Join to get type IDs
    fact = pokemon.alias("p").join(
        types.alias("t1"),
        F.col("p.type_1") == F.col("t1.type_name"),
        "left"
    ).join(
        types.alias("t2"),
        F.col("p.type_2") == F.col("t2.type_name"),
        "left"
    ).select(
        F.col("p.pokemon_id"),
        F.col("p.hp"),
        F.col("p.attack"),
        F.col("p.defense"),
        F.col("p.sp_atk"),
        F.col("p.sp_def"),
        F.col("p.speed"),
        (F.col("p.hp") + F.col("p.attack") + F.col("p.defense") +
         F.col("p.sp_atk") + F.col("p.sp_def") + F.col("p.speed")).alias("total_base_stats"),
        F.col("t1.type_id").alias("type_1_id"),
        F.col("t2.type_id").alias("type_2_id"),
        F.lit(datetime.utcnow().isoformat()).alias("loaded_at")
    )

    fact.write.format("delta").mode("overwrite").saveAsTable(GOLD_FACT_POKEMON_STATS)
    print(f"Built {GOLD_FACT_POKEMON_STATS} with {fact.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics View: v_pokemon_analytics
# MAGIC Pre-joined view for Genie, dashboards, and ad-hoc queries

# COMMAND ----------

def build_analytics_view(spark):
    """
    Build v_pokemon_analytics:
    Pre-joins fact + dimensions so analytics tools see names instead of IDs.
    """
    spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_V_POKEMON_ANALYTICS} AS
    SELECT
        p.pokemon_id,
        p.name,
        p.height,
        p.weight,
        p.base_experience,
        f.hp,
        f.attack,
        f.defense,
        f.sp_atk,
        f.sp_def,
        f.speed,
        f.total_base_stats,
        t1.type_name AS type_1,
        t2.type_name AS type_2
    FROM {GOLD_FACT_POKEMON_STATS} f
    JOIN {GOLD_DIM_POKEMON} p ON f.pokemon_id = p.pokemon_id
    LEFT JOIN {GOLD_DIM_TYPE} t1 ON f.type_1_id = t1.type_id
    LEFT JOIN {GOLD_DIM_TYPE} t2 ON f.type_2_id = t2.type_id
    """)
    print(f"Built {GOLD_V_POKEMON_ANALYTICS} view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Star Schema Build

# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    print("building Gold star schema...")

    # Build dimensions first (fact depends on dim_type)
    build_dim_type(spark)
    build_dim_ability(spark)
    build_dim_pokemon(spark)

    # Build fact table
    build_fact_pokemon_stats(spark)

    # Build analytics view (for Genie/dashboards)
    build_analytics_view(spark)

    print("gold star schema build complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Gold Tables

# COMMAND ----------

display(spark.table(GOLD_DIM_TYPE).orderBy("type_id").limit(10))

# COMMAND ----------

display(spark.table(GOLD_DIM_ABILITY).orderBy("ability_id").limit(10))

# COMMAND ----------

display(spark.table(GOLD_DIM_POKEMON).orderBy("pokemon_id").limit(10))

# COMMAND ----------

display(spark.table(GOLD_FACT_POKEMON_STATS).orderBy("pokemon_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analytics View (use this for Genie/dashboards)

# COMMAND ----------

display(spark.table(GOLD_V_POKEMON_ANALYTICS).orderBy("pokemon_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Analytical Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 Pokemon by Total Base Stats

# COMMAND ----------

display(
    spark.table(GOLD_FACT_POKEMON_STATS).alias("f")
    .join(spark.table(GOLD_DIM_POKEMON).alias("d"), "pokemon_id")
    .select("d.name", "f.total_base_stats", "f.hp", "f.attack", "f.defense", "f.sp_atk", "f.sp_def", "f.speed")
    .orderBy(F.desc("total_base_stats"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Stats by Type

# COMMAND ----------

display(
    spark.table(GOLD_FACT_POKEMON_STATS).alias("f")
    .join(spark.table(GOLD_DIM_TYPE).alias("t"), F.col("f.type_1_id") == F.col("t.type_id"))
    .groupBy("t.type_name")
    .agg(
        F.round(F.avg("f.total_base_stats"), 1).alias("avg_total"),
        F.round(F.avg("f.attack"), 1).alias("avg_attack"),
        F.round(F.avg("f.defense"), 1).alias("avg_defense"),
        F.count("*").alias("count")
    )
    .orderBy(F.desc("avg_total"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pokemon Count by Primary Type

# COMMAND ----------

display(
    spark.table(GOLD_FACT_POKEMON_STATS).alias("f")
    .join(spark.table(GOLD_DIM_TYPE).alias("t"), F.col("f.type_1_id") == F.col("t.type_id"))
    .groupBy("t.type_name")
    .count()
    .orderBy(F.desc("count"))
)
