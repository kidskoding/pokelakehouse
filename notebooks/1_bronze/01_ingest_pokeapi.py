# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest PokeAPI
# MAGIC Raw JSON ingestion from PokeAPI to Delta tables

import json
import requests
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

# COMMAND ----------

API_BASE_URL = "https://pokeapi.co/api/v2"
POKEMON_LIMIT = 151
BRONZE_PATH = "dbfs:/pokelakehouse/bronze"

config = {
    "api": {"base_url": API_BASE_URL, "limit": POKEMON_LIMIT},
    "storage": {"bronze_path": BRONZE_PATH}
}

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

"""
    Main ingestion for pokemon data.
    - Fetch all pokemon details
    - Convert to DataFrame with raw JSON
    - Add metadata: ingestion_timestamp, source_url
    - Write to bronze_path/pokemon as Delta
"""
def ingest_pokemon(spark, config: dict):
    base_url = config["api"]["base_url"]
    limit = config["api"]["limit"]
    bronze_path = config["storage"]["bronze_path"]

    pokemon_urls = fetch_pokemon_list(base_url, limit)

    pokemon_records = []
    for url in pokemon_urls:
        details = fetch_pokemon_details(url)
        pokemon_records.append({
            "pokemon_id": details["id"],
            "name": details["name"],
            "raw_json": json.dumps(details),
            "source_url": url,
            "ingestion_timestamp": datetime.utcnow().isoformat()
        })

    df = spark.createDataFrame(pokemon_records)
    df.write.format("delta").mode("overwrite").save(f"{bronze_path}/pokemon")
    print(f"Ingested {len(pokemon_records)} pokemon to {bronze_path}/pokemon")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Types

# COMMAND ----------

def ingest_types(spark, config: dict):
    base_url = config["api"]["base_url"]
    bronze_path = config["storage"]["bronze_path"]

    response = requests.get(f"{base_url}/type")
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
            "source_url": t["url"],
            "ingestion_timestamp": datetime.utcnow().isoformat()
        })

    df = spark.createDataFrame(type_records)
    df.write.format("delta").mode("overwrite").save(f"{bronze_path}/types")
    print(f"Ingested {len(type_records)} types to {bronze_path}/types")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Abilities

# COMMAND ----------

"""
    Ingest /ability endpoint to bronze_path/abilities
    - Add metadata columns
    - Write as Delta
"""
def ingest_abilities(spark, config: dict):
    base_url = config["api"]["base_url"]
    bronze_path = config["storage"]["bronze_path"]

    response = requests.get(f"{base_url}/ability?limit=500")
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
            "source_url": a["url"],
            "ingestion_timestamp": datetime.utcnow().isoformat()
        })

    df = spark.createDataFrame(ability_records)
    df.write.format("delta").mode("overwrite").save(f"{bronze_path}/abilities")
    print(f"Ingested {len(ability_records)} abilities to {bronze_path}/abilities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Ingestion

# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    print("starting Bronze layer ingestion...")
    ingest_pokemon(spark, config)
    ingest_types(spark, config)
    ingest_abilities(spark, config)
    print("bronze layer ingestion complete!")
