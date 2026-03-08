# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Gate
# MAGIC Assertions that MUST pass before Gold layer runs

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

from constants import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check Functions

# COMMAND ----------

def check_row_count(spark, table_name: str, min_rows: int) -> tuple[bool, str]:
    """Check that table has at least min_rows."""
    count = spark.table(table_name).count()
    passed = count >= min_rows
    msg = f"{table_name}: row count = {count} (min: {min_rows})"
    return passed, msg

# COMMAND ----------

def check_no_nulls(spark, table_name: str, columns: list) -> tuple[bool, str]:
    """Check that specified columns have no null values."""
    df = spark.table(table_name)
    failures = []

    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            failures.append(f"{col}={null_count} nulls")

    passed = len(failures) == 0
    if passed:
        msg = f"{table_name}: no nulls in {columns}"
    else:
        msg = f"{table_name}: NULL CHECK FAILED - {', '.join(failures)}"

    return passed, msg

# COMMAND ----------

def check_positive_values(spark, table_name: str, columns: list) -> tuple[bool, str]:
    """Check that specified columns have values > 0."""
    df = spark.table(table_name)
    failures = []

    for col in columns:
        non_positive = df.filter((F.col(col).isNull()) | (F.col(col) <= 0)).count()
        if non_positive > 0:
            failures.append(f"{col}={non_positive} non-positive")

    passed = len(failures) == 0
    if passed:
        msg = f"{table_name}: all values positive in {columns}"
    else:
        msg = f"{table_name}: POSITIVE CHECK FAILED - {', '.join(failures)}"

    return passed, msg

# COMMAND ----------

def check_no_duplicates(spark, table_name: str, key_column: str) -> tuple[bool, str]:
    """Check that key column has no duplicate values."""
    df = spark.table(table_name)
    total = df.count()
    distinct = df.select(key_column).distinct().count()

    passed = total == distinct
    if passed:
        msg = f"{table_name}: no duplicates on {key_column}"
    else:
        msg = f"{table_name}: DUPLICATE CHECK FAILED - {total - distinct} duplicates on {key_column}"

    return passed, msg

# COMMAND ----------

def check_no_data_loss(spark, bronze_table: str, silver_table: str) -> tuple[bool, str]:
    """Check that silver has >= rows than bronze (no data loss during transform)."""
    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()

    passed = silver_count >= bronze_count
    if passed:
        msg = f"{silver_table}: no data loss (bronze={bronze_count}, silver={silver_count})"
    else:
        msg = f"{silver_table}: DATA LOSS - bronze={bronze_count}, silver={silver_count}, lost={bronze_count - silver_count}"

    return passed, msg

# COMMAND ----------

def check_referential_integrity(spark, fact_table: str, fact_col: str, dim_table: str, dim_col: str) -> tuple[bool, str]:
    """Check that all values in fact_col exist in dim_col."""
    fact_values = spark.table(fact_table).select(fact_col).filter(F.col(fact_col).isNotNull()).distinct()
    dim_values = spark.table(dim_table).select(dim_col).distinct()

    orphans = fact_values.subtract(dim_values).count()

    passed = orphans == 0
    if passed:
        msg = f"{fact_table}.{fact_col}: all values exist in {dim_table}.{dim_col}"
    else:
        msg = f"{fact_table}.{fact_col}: REFERENTIAL INTEGRITY FAILED - {orphans} orphan values"

    return passed, msg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Checks

# COMMAND ----------

def run_quality_gate(spark):
    """
    Run all quality checks. Raise Exception if ANY fail.

    Checks:
    1. Row counts meet minimums
    2. No nulls on required columns
    3. Stats are positive
    4. No duplicate keys
    5. No data loss from bronze to silver
    6. Referential integrity (pokemon types exist in types table)
    """
    results = []

    print("=" * 60)
    print("QUALITY GATE: Starting checks...")
    print("=" * 60)

    # 1. Row count checks
    results.append(check_row_count(spark, SILVER_POKEMON, 100))
    results.append(check_row_count(spark, SILVER_TYPES, 10))
    results.append(check_row_count(spark, SILVER_ABILITIES, 50))

    # 2. No nulls on required columns
    results.append(check_no_nulls(spark, SILVER_POKEMON, ["pokemon_id", "name", "type_1"]))
    results.append(check_no_nulls(spark, SILVER_TYPES, ["type_id", "type_name"]))
    results.append(check_no_nulls(spark, SILVER_ABILITIES, ["ability_id", "ability_name"]))

    # 3. Stats are positive
    results.append(check_positive_values(spark, SILVER_POKEMON, ["hp", "attack", "defense", "sp_atk", "sp_def", "speed"]))

    # 4. No duplicate keys
    results.append(check_no_duplicates(spark, SILVER_POKEMON, "pokemon_id"))
    results.append(check_no_duplicates(spark, SILVER_TYPES, "type_id"))
    results.append(check_no_duplicates(spark, SILVER_ABILITIES, "ability_id"))

    # 5. No data loss
    results.append(check_no_data_loss(spark, BRONZE_POKEMON, SILVER_POKEMON))
    results.append(check_no_data_loss(spark, BRONZE_TYPES, SILVER_TYPES))
    results.append(check_no_data_loss(spark, BRONZE_ABILITIES, SILVER_ABILITIES))

    # 6. Referential integrity - pokemon type_1 must exist in types table
    results.append(check_referential_integrity(spark, SILVER_POKEMON, "type_1", SILVER_TYPES, "type_name"))

    # Print results
    print("\n" + "-" * 60)
    print("RESULTS:")
    print("-" * 60)

    passed_count = 0
    failed_checks = []

    for passed, msg in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {msg}")
        if passed:
            passed_count += 1
        else:
            failed_checks.append(msg)

    # Summary
    total = len(results)
    print("\n" + "=" * 60)
    print(f"SUMMARY: {passed_count}/{total} checks passed")
    print("=" * 60)

    # Raise exception if any failed
    if failed_checks:
        error_msg = f"Quality gate FAILED: {len(failed_checks)} check(s) failed:\n" + "\n".join(f"  - {msg}" for msg in failed_checks)
        raise Exception(error_msg)

    print("\nQuality gate PASSED - Gold layer can proceed!")
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Quality Gate

# COMMAND ----------

if __name__ == "__main__" or "dbutils" in dir():
    run_quality_gate(spark)
