# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: land the legacy extract as-is
# MAGIC Rule of a financial migration: **land every source row unchanged** before transforming anything.
# MAGIC All columns stay as strings; we add audit columns (source file, load time, row hash, source row id)
# MAGIC so any target row can be traced back to its exact source row.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Optional: try to pull the file straight from ONRR. If outbound internet is blocked in your
# workspace, upload monthly_revenue.csv to the volume by hand (README step 3) and skip this cell.
import os, urllib.request
SRC_URL = "https://revenuedata.doi.gov/downloads/monthly_revenue.csv"
if not os.path.exists(RAW_FILE):
    try:
        urllib.request.urlretrieve(SRC_URL, RAW_FILE)
        print("Downloaded from ONRR")
    except Exception as e:
        print(f"Download failed ({e}). Upload the CSV to {RAW_FILE} manually.")
else:
    print("Raw file already in volume")

# COMMAND ----------

import re
from pyspark.sql import functions as F

raw = (spark.read
       .option("header", True)
       .option("inferSchema", False)     # keep everything as string in bronze
       .option("multiLine", True)
       .option("escape", '"')
       .csv(RAW_FILE))

def snake(c):
    return re.sub(r"[^0-9a-z]+", "_", c.strip().lower()).strip("_")

raw = raw.toDF(*[snake(c) for c in raw.columns])
print("Source columns:", raw.columns)

missing = [c for c in REQUIRED_COLS if c not in raw.columns]
assert not missing, f"Source is missing required columns {missing}. Found: {raw.columns}"

for c in OPTIONAL_COLS:
    if c not in raw.columns:
        raw = raw.withColumn(c, F.lit(None).cast("string"))

src_cols = raw.columns
bronze = (raw
    .withColumn("_source_file", F.lit(RAW_FILE))
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in src_cols]), 256))
    .withColumn("_src_row_id", F.monotonically_increasing_id()))

(bronze.write.mode("overwrite").option("overwriteSchema", True)
       .saveAsTable("bronze_monthly_revenue"))

print(f"Bronze rows: {spark.table('bronze_monthly_revenue').count():,}")

# COMMAND ----------

# MAGIC %md ### Quick profile of the legacy data (look for surprises before writing rules)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT land_class, land_category, revenue_type, count(*) AS rows
# MAGIC FROM bronze_monthly_revenue
# MAGIC GROUP BY ALL ORDER BY rows DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, revenue FROM bronze_monthly_revenue LIMIT 20
