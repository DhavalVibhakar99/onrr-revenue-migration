# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: type, cleanse, validate
# MAGIC Every bronze row ends up in **exactly one** of two tables:
# MAGIC * `silver_revenue_txn`: passed validation, typed, standardised
# MAGIC * `silver_revenue_rejects`: failed at least one rule, with reason codes
# MAGIC
# MAGIC Nothing is silently dropped. That invariant is what makes reconciliation possible.
# MAGIC
# MAGIC | Code | Rule | Action |
# MAGIC |---|---|---|
# MAGIC | V001 | date not parseable | reject |
# MAGIC | V002 | revenue amount not numeric | reject |
# MAGIC | V003 | revenue type missing | reject |
# MAGIC | V004 | land class missing | reject |
# MAGIC | V005 | date in the future | reject |
# MAGIC | D001 | commodity / product blank → 'Not Applicable' | default (not a reject) |
# MAGIC | W001 | exact duplicate source row | warning flag only (aggregated data can repeat legitimately) |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_typed AS
SELECT
  _src_row_id,
  _row_hash,
  date    AS revenue_date_raw,
  revenue AS revenue_raw,
  coalesce(
    to_date(try_to_timestamp(trim(date), 'yyyy-MM-dd')),
    to_date(try_to_timestamp(trim(date), 'M/d/yyyy')),
    to_date(try_to_timestamp(trim(date), 'MM/dd/yyyy')),
    to_date(try_to_timestamp(trim(date), 'yyyy-MM-dd HH:mm:ss'))
  ) AS revenue_date,
  {REVENUE_PARSE_SQL.format(c='revenue')} AS revenue_amount,
  nullif(trim(land_class), '')         AS land_class,
  nullif(trim(land_category), '')      AS land_category,
  nullif(trim(state), '')              AS state,
  nullif(trim(county), '')             AS county,
  nullif(trim(fips_code), '')          AS fips_code,
  nullif(trim(offshore_region), '')    AS offshore_region,
  nullif(trim(revenue_type), '')       AS revenue_type,
  coalesce(nullif(trim(mineral_lease_type), ''), 'Not Applicable') AS mineral_lease_type,
  coalesce(nullif(trim(commodity), ''), 'Not Applicable')          AS commodity,
  coalesce(nullif(trim(product), ''), 'Not Applicable')            AS product
FROM bronze_monthly_revenue
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_validated AS
# MAGIC SELECT *,
# MAGIC   filter(array(
# MAGIC     CASE WHEN revenue_date IS NULL            THEN 'V001_INVALID_DATE' END,
# MAGIC     CASE WHEN revenue_amount IS NULL          THEN 'V002_INVALID_AMOUNT' END,
# MAGIC     CASE WHEN revenue_type IS NULL            THEN 'V003_MISSING_REVENUE_TYPE' END,
# MAGIC     CASE WHEN land_class IS NULL              THEN 'V004_MISSING_LAND_CLASS' END,
# MAGIC     CASE WHEN revenue_date > current_date()   THEN 'V005_FUTURE_DATE' END
# MAGIC   ), x -> x IS NOT NULL) AS reject_reasons,
# MAGIC   count(*) OVER (PARTITION BY _row_hash) > 1 AS w001_duplicate_flag,
# MAGIC   -- deterministic business key: content hash + occurrence number among identical rows
# MAGIC   concat(_row_hash, '-', row_number() OVER (PARTITION BY _row_hash ORDER BY _src_row_id)) AS txn_id
# MAGIC FROM v_typed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_revenue_txn AS
# MAGIC SELECT txn_id, _src_row_id, revenue_date, revenue_amount,
# MAGIC        land_class, land_category, state, county, fips_code, offshore_region,
# MAGIC        revenue_type, mineral_lease_type, commodity, product,
# MAGIC        w001_duplicate_flag, current_timestamp() AS _silver_ts
# MAGIC FROM v_validated
# MAGIC WHERE size(reject_reasons) = 0;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_revenue_rejects AS
# MAGIC SELECT txn_id, _src_row_id, revenue_date_raw, revenue_raw, revenue_amount,
# MAGIC        land_class, revenue_type, commodity, reject_reasons, current_timestamp() AS _silver_ts
# MAGIC FROM v_validated
# MAGIC WHERE size(reject_reasons) > 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation summary
# MAGIC SELECT 'valid' AS outcome, count(*) AS rows, sum(revenue_amount) AS revenue FROM silver_revenue_txn
# MAGIC UNION ALL
# MAGIC SELECT 'rejected', count(*), sum(revenue_amount) FROM silver_revenue_rejects

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT reason, count(*) AS rows
# MAGIC FROM silver_revenue_rejects LATERAL VIEW explode(reject_reasons) t AS reason
# MAGIC GROUP BY reason ORDER BY rows DESC
