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

from constants import *

# COMMAND ----------

API_BASE_URL = "https://pokeapi.co/api/v2"
POKEMON_LIMIT = 151

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Unity Catalog

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception:
    pass

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Pokemon

# COMMAND ----------

def fetch_pokemon_list(base_url: str, limit: int) -> list:
    url = f"{base_url}/pokemon?limit={limit}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return [item["url"] for item in data["results"]]

# COMMAND ----------

def fetch_pokemon_details(url: str) -> dict:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# COMMAND ----------

def ingest_pokemon(spark):
    """Fetch all pokemon and write to Unity Catalog bronze.pokemon table."""
    pokemon_urls = fetch_pokemon_list(API_BASE_URL, POKEMON_LIMIT)

    pokemon_records = []
    for url in pokemon_urls:
        details = fetch_pokemon_details(url)
        pokemon_records.append({
            "pokemon_id": details["id"],
            "name": details["name"],
            "raw_json": json.dumps(details),
            "ingestion_timestamp": datetime.utcnow().isoformat()
        })

    table_name = BRONZE_POKEMON
    df = spark.createDataFrame(pokemon_records)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Ingested {len(pokemon_records)} pokemon to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Types

# COMMAND ----------

def ingest_types(spark):
    """Fetch all types and write to Unity Catalog bronze.types table."""
    response = requests.get(f"{API_BASE_URL}/type")
    response.raise_for_status()
    type_list = response.json()["results"]

    type_records = []
    for t in type_list:
        detail_response = requests.get(t["url"])
        detail_response.raise_for_status()
        details = detail_response.json()
        type_records.append({
            "type_id": details["id"],
            "name": details["name"],
            "raw_json": json.dumps(details),
            "ingestion_timestamp": datetime.utcnow().isoformat()
        })

    table_name = BRONZE_TYPES
    df = spark.createDataFrame(type_records)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Ingested {len(type_records)} types to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Abilities

# COMMAND ----------

def ingest_abilities(spark):
    """Fetch all abilities and write to Unity Catalog bronze.abilities table."""
    response = requests.get(f"{API_BASE_URL}/ability?limit=500")
    response.raise_for_status()
    ability_list = response.json()["results"]

    ability_records = []
    for a in ability_list:
        detail_response = requests.get(a["url"])
        detail_response.raise_for_status()
        details = detail_response.json()
        ability_records.append({
            "ability_id": details["id"],
            "name": details["name"],
            "raw_json": json.dumps(details),
            "ingestion_timestamp": datetime.utcnow().isoformat()
        })

    table_name = BRONZE_ABILITIES
    df = spark.createDataFrame(ability_records)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Ingested {len(ability_records)} abilities to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Ingestion

# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    print("starting Bronze layer ingestion...")
    ingest_pokemon(spark)
    ingest_types(spark)
    ingest_abilities(spark)
    print("bronze layer ingestion complete!")

# COMMAND ----------

# DBTITLE 1,Browse all bronze pokemon
display(spark
        .table(BRONZE_POKEMON)
        .select("pokemon_id", "name", "raw_json", "ingestion_timestamp")
        .orderBy("pokemon_id")
        .limit(10))

# COMMAND ----------

display(spark
        .table(BRONZE_ABILITIES)
        .select("ability_id", "name", "raw_json", "ingestion_timestamp")
        .orderBy("ability_id")
        .limit(10))

# COMMAND ----------

display(spark
        .table(BRONZE_TYPES)
        .select("type_id", "name", "raw_json", "ingestion_timestamp")
        .orderBy("type_id")
        .limit(10))
