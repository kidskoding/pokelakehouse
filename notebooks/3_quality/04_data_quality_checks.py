# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Gate
# MAGIC Assertions that MUST pass before Gold layer runs

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Load config
# TODO: Read from configs/pipeline_config.json
config = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check Functions

# COMMAND ----------

def check_row_count(df, table_name: str, min_rows: int) -> bool:
    """Check that table has at least min_rows"""
    # TODO: Implement row count check
    pass

# COMMAND ----------

def check_no_nulls(df, columns: list, table_name: str) -> bool:
    """Check that specified columns have no null values"""
    # TODO: Implement null check
    pass

# COMMAND ----------

def check_positive_values(df, columns: list, table_name: str) -> bool:
    """Check that specified columns have values > 0"""
    # TODO: Implement positive value check
    pass

# COMMAND ----------

def check_no_data_loss(bronze_count: int, silver_count: int) -> bool:
    """Check that silver has >= rows than bronze (no data loss)"""
    # TODO: Implement data loss check
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Checks

# COMMAND ----------

def run_quality_gate(spark, config: dict):
    """
    Run all quality checks. Raise Exception if ANY fail.

    Checks:
    1. silver/pokemon has > 100 rows
    2. No nulls on pokemon_id, name
    3. hp, attack, defense are all > 0
    4. type_1 is never null
    5. Silver row count >= Bronze row count

    Print quality report summary at end.
    """
    # TODO: Implement quality gate
    # IMPORTANT: Must raise Exception on failure, not just print warning

    results = []

    # TODO: Run each check, collect results

    # TODO: Print summary report

    # TODO: If any check failed, raise Exception("Quality gate failed: ...")

    pass

# COMMAND ----------

if __name__ == "__main__" or dbutils:
    # TODO: Call run_quality_gate(spark, config)
    pass
