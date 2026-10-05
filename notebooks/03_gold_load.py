# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: load the target star schema with MERGE
# MAGIC Dimensions get deterministic hash surrogate keys, so re-runs produce the same keys.
# MAGIC The fact load is an idempotent `MERGE` (insert new, update changed amounts, delete rows that
# MAGIC disappeared from source). Running this notebook twice changes nothing the second time.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_location (
# MAGIC   location_key BIGINT, land_class STRING, land_category STRING, state STRING,
# MAGIC   county STRING, fips_code STRING, offshore_region STRING);
# MAGIC
# MAGIC MERGE INTO dim_location t
# MAGIC USING (
# MAGIC   SELECT DISTINCT xxhash64(land_class, land_category, coalesce(state,'~'), coalesce(county,'~'),
# MAGIC                            coalesce(fips_code,'~'), coalesce(offshore_region,'~')) AS location_key,
# MAGIC          land_class, land_category, state, county, fips_code, offshore_region
# MAGIC   FROM silver_revenue_txn) s
# MAGIC ON t.location_key = s.location_key
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_commodity (
# MAGIC   commodity_key BIGINT, mineral_lease_type STRING, commodity STRING, product STRING);
# MAGIC
# MAGIC MERGE INTO dim_commodity t
# MAGIC USING (
# MAGIC   SELECT DISTINCT xxhash64(mineral_lease_type, commodity, product) AS commodity_key,
# MAGIC          mineral_lease_type, commodity, product
# MAGIC   FROM silver_revenue_txn) s
# MAGIC ON t.commodity_key = s.commodity_key
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_revenue_type (revenue_type_key BIGINT, revenue_type STRING);
# MAGIC
# MAGIC MERGE INTO dim_revenue_type t
# MAGIC USING (SELECT DISTINCT xxhash64(revenue_type) AS revenue_type_key, revenue_type FROM silver_revenue_txn) s
# MAGIC ON t.revenue_type_key = s.revenue_type_key
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Date dimension with U.S. federal fiscal year (Oct 1 to Sep 30)
# MAGIC CREATE OR REPLACE TABLE dim_date AS
# MAGIC SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC        d AS calendar_date,
# MAGIC        trunc(d, 'MM') AS month_start,
# MAGIC        year(d) AS calendar_year,
# MAGIC        quarter(d) AS calendar_quarter,
# MAGIC        CASE WHEN month(d) >= 10 THEN year(d) + 1 ELSE year(d) END AS fiscal_year
# MAGIC FROM (SELECT explode(sequence(min(revenue_date), max(revenue_date), interval 1 day)) AS d
# MAGIC       FROM silver_revenue_txn)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS fact_revenue_txn (
# MAGIC   txn_id STRING, date_key INT, location_key BIGINT, commodity_key BIGINT,
# MAGIC   revenue_type_key BIGINT, revenue_amount DECIMAL(18,2), _src_row_id BIGINT,
# MAGIC   _loaded_ts TIMESTAMP, _updated_ts TIMESTAMP);
# MAGIC
# MAGIC MERGE INTO fact_revenue_txn t
# MAGIC USING (
# MAGIC   SELECT txn_id,
# MAGIC          CAST(date_format(revenue_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC          xxhash64(land_class, land_category, coalesce(state,'~'), coalesce(county,'~'),
# MAGIC                   coalesce(fips_code,'~'), coalesce(offshore_region,'~')) AS location_key,
# MAGIC          xxhash64(mineral_lease_type, commodity, product) AS commodity_key,
# MAGIC          xxhash64(revenue_type) AS revenue_type_key,
# MAGIC          revenue_amount, _src_row_id
# MAGIC   FROM silver_revenue_txn) s
# MAGIC ON t.txn_id = s.txn_id
# MAGIC WHEN MATCHED AND t.revenue_amount <> s.revenue_amount THEN
# MAGIC   UPDATE SET t.revenue_amount = s.revenue_amount, t._updated_ts = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (txn_id, date_key, location_key, commodity_key, revenue_type_key, revenue_amount,
# MAGIC           _src_row_id, _loaded_ts, _updated_ts)
# MAGIC   VALUES (s.txn_id, s.date_key, s.location_key, s.commodity_key, s.revenue_type_key,
# MAGIC           s.revenue_amount, s._src_row_id, current_timestamp(), NULL)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta keeps a full audit trail of every load (useful in interviews: "how do you roll back a bad load?")
# MAGIC DESCRIBE HISTORY fact_revenue_txn
