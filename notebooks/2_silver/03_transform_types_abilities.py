# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Transform Types & Abilities
# MAGIC Flatten and clean types/abilities data from Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType
from datetime import datetime

# COMMAND ----------

from constants import *

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Types

# COMMAND ----------

def transform_types(spark):
    """
    Transform bronze types to silver:
    - Extract: type_id, type_name, damage_double_from, damage_double_to, etc.
    - Add processed_at
    - Write to silver.types as Delta
    """
    df = spark.table(BRONZE_TYPES)

    json_schema = spark.read.json(df.select("raw_json").rdd.map(lambda r: r[0])).schema
    df = df.withColumn("parsed", F.from_json(F.col("raw_json"), json_schema))

    df = df.select(
        F.col("type_id"),
        F.col("parsed.name").alias("type_name"),
        
        F.expr("transform(parsed.damage_relations.double_damage_from, x -> x.name)").alias("double_damage_from"),
        F.expr("transform(parsed.damage_relations.double_damage_to, x -> x.name)").alias("double_damage_to"),
        F.expr("transform(parsed.damage_relations.half_damage_from, x -> x.name)").alias("half_damage_from"),
        F.expr("transform(parsed.damage_relations.half_damage_to, x -> x.name)").alias("half_damage_to"),
        F.expr("transform(parsed.damage_relations.no_damage_from, x -> x.name)").alias("no_damage_from"),
        F.expr("transform(parsed.damage_relations.no_damage_to, x -> x.name)").alias("no_damage_to"),
        
        F.lit(datetime.utcnow().isoformat()).alias("processed_at")
    )

    table_name = SILVER_TYPES

    if spark.catalog.tableExists(table_name):
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, table_name)

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.type_id = source.type_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print(f"Merged {df.count()} records into {table_name}")
    else:
        df.write.format("delta").saveAsTable(table_name)
        print(f"Created {table_name} with {df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Abilities

# COMMAND ----------

def transform_abilities(spark):
    """
    Transform bronze abilities to silver:
    - Extract: ability_id, ability_name, effect_short, is_main_series
    - Add processed_at
    - Write to silver.abilities as Delta
    """
    df = spark.table(BRONZE_ABILITIES)

    json_schema = spark.read.json(df.select("raw_json").rdd.map(lambda r: r[0])).schema
    df = df.withColumn("parsed", F.from_json(F.col("raw_json"), json_schema))

    df = df.select(
        F.col("ability_id"),
        F.col("parsed.name").alias("ability_name"),
        
        F.expr("""
            filter(parsed.effect_entries, x -> x.language.name = 'en')[0].short_effect
        """).alias("effect_short"),
        F.expr("""
            filter(parsed.flavor_text_entries, x -> x.language.name = 'en')[0].flavor_text
        """).alias("flavor_text"),
        
        F.col("parsed.is_main_series").cast(BooleanType()).alias("is_main_series"),
        F.lit(datetime.utcnow().isoformat()).alias("processed_at")
    )

    table_name = SILVER_ABILITIES
    if spark.catalog.tableExists(table_name):
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, table_name)

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.ability_id = source.ability_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print(f"Merged {df.count()} records into {table_name}")
    else:
        df.write.format("delta").saveAsTable(table_name)
        print(f"Created {table_name} with {df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Transforms

# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    print("starting Silver types/abilities transformation...")
    transform_types(spark)
    transform_abilities(spark)
    print("silver types/abilities transformation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Silver Data

# COMMAND ----------

display(spark
        .table(SILVER_TYPES)
        .orderBy("type_id")
        .limit(10))

# COMMAND ----------

display(spark
        .table(SILVER_ABILITIES)
        .orderBy("ability_id")
        .limit(10))
