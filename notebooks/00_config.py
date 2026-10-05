# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Config
# MAGIC Shared settings, loaded by every other notebook with `%run ./00_config`.
# MAGIC
# MAGIC **Source:** ONRR Monthly Revenue (U.S. Department of the Interior). Royalties, rents, bonuses and
# MAGIC other revenue on federal and Native American leases, January 2003 to present.

# COMMAND ----------

CATALOG = "workspace"            # default catalog in Databricks Free Edition
SCHEMA = "onrr_migration"
VOLUME = "raw"
RAW_FILE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/monthly_revenue.csv"

# Columns the migration cannot run without (after header normalisation to snake_case)
REQUIRED_COLS = ["date", "land_class", "land_category", "revenue_type",
                 "mineral_lease_type", "commodity", "revenue"]
# Columns that are carried if present, created as NULL if absent
OPTIONAL_COLS = ["state", "county", "fips_code", "offshore_region", "product"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Amount parsing rule shared by the staging load and the reconciliation re-read,
# so source and target totals are computed the same way.
REVENUE_PARSE_SQL = """
try_cast(
  CASE WHEN left(trim({c}), 1) = '(' THEN concat('-', regexp_replace({c}, '[()$, ]', ''))
       ELSE regexp_replace({c}, '[$, ]', '') END
  AS DECIMAL(18,2))
"""

print(f"Using {CATALOG}.{SCHEMA} | raw file: {RAW_FILE}")
