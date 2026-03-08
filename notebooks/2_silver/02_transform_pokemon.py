# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Transform Pokemon
# MAGIC Flatten and clean pokemon data from Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from datetime import datetime

# COMMAND ----------

from constants import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Silver Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

def read_bronze_pokemon(spark):
    """Read raw pokemon data from bronze layer and parse JSON."""
    df = spark.table(BRONZE_POKEMON)

    json_sample = df.select("raw_json").limit(1).collect()[0][0]
    json_schema = F.schema_of_json(F.lit(json_sample))
    
    df = df.withColumn("parsed", F.from_json(F.col("raw_json"), json_schema))
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten Stats

# COMMAND ----------

def flatten_stats(df):
    """
    Flatten stats array into individual columns:
    - hp, attack, defense, sp_atk, sp_def, speed
    """
    # Stats array structure: [{base_stat: int, stat: {name: str}}]
    # Extract each stat by filtering on stat.name
    df = df.withColumn("stats_array", F.col("parsed.stats"))

    stat_names = ["hp", "attack", "defense", "special-attack", "special-defense", "speed"]
    col_names = ["hp", "attack", "defense", "sp_atk", "sp_def", "speed"]

    for stat_name, col_name in zip(stat_names, col_names):
        df = df.withColumn(
            col_name,
            F.expr(f"get(filter(stats_array, x -> x.stat.name = '{stat_name}'), 0).base_stat").cast(IntegerType())
        )

    return df.drop("stats_array")

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
    # Types array structure: [{slot: int, type: {name: str, url: str}}]
    df = df.withColumn("types_array", F.col("parsed.types"))

    df = df.withColumn(
        "type_1",
        F.expr("get(filter(types_array, x -> x.slot = 1), 0).type.name")
    )
    df = df.withColumn(
        "type_2",
        F.expr("get(filter(types_array, x -> x.slot = 2), 0).type.name")
    )

    return df.drop("types_array")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten Abilities

# COMMAND ----------

def flatten_abilities(df):
    """
    Flatten abilities array into:
    - ability_1, ability_2, ability_3 (all nullable except ability_1)
    """
    # Abilities array structure: [{ability: {name: str}, is_hidden: bool, slot: int}]
    df = df.withColumn("abilities_array", F.col("parsed.abilities"))

    for slot in [1, 2, 3]:
        df = df.withColumn(
            f"ability_{slot}",
            F.expr(f"get(filter(abilities_array, x -> x.slot = {slot}), 0).ability.name")
        )

    return df.drop("abilities_array")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Transform

# COMMAND ----------

def transform_pokemon(spark):
    """
    Main transformation pipeline:
    1. Read bronze pokemon
    2. Flatten stats, types, abilities
    3. Add processed_at timestamp
    4. Write to silver.pokemon as Delta with MERGE
    """
    
    # Read and parse bronze data
    df = read_bronze_pokemon(spark)

    # Flatten nested structures
    df = flatten_stats(df)
    df = flatten_types(df)
    df = flatten_abilities(df)

    # Select final columns
    df = df.select(
        F.col("pokemon_id"),
        F.col("parsed.name").alias("name"),
        F.col("parsed.height").cast(IntegerType()).alias("height"),
        F.col("parsed.weight").cast(IntegerType()).alias("weight"),
        F.col("parsed.base_experience").cast(IntegerType()).alias("base_experience"),
        "hp", "attack", "defense", "sp_atk", "sp_def", "speed",
        "type_1", "type_2",
        "ability_1", "ability_2", "ability_3",
        F.lit(datetime.utcnow().isoformat()).alias("processed_at")
    )

    table_name = SILVER_POKEMON

    # Use MERGE for upsert (as per project conventions)
    if spark.catalog.tableExists(table_name):
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, table_name)

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.pokemon_id = source.pokemon_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print(f"Merged {df.count()} records into {table_name}")
    else:
        df.write.format("delta").saveAsTable(table_name)
        print(f"Created {table_name} with {df.count()} records")

    # Z-ORDER by pokemon_id for query performance
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (pokemon_id)")
    print(f"Applied Z-ORDER on pokemon_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Transform

# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    print("starting Silver pokemon transformation...")
    transform_pokemon(spark)
    print("silver pokemon transformation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Silver Data

# COMMAND ----------

display(spark
        .table(SILVER_POKEMON)
        .orderBy("pokemon_id")
        .limit(10))
