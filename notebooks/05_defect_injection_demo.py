# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Prove the reconciliation catches real migration defects
# MAGIC A reconciliation that always says PASS proves nothing. Here we copy the fact table, break it
# MAGIC the way real migrations break, and show which checks catch each defect:
# MAGIC
# MAGIC | Defect injected | Expected to fail |
# MAGIC |---|---|
# MAGIC | 3 transactions dropped (e.g. a failed batch) | C03, C05, C07, C04 |
# MAGIC | 1 amount changed by $0.01 (rounding / type bug) | C04, C05, C06 |
# MAGIC | 1 duplicate row inserted (re-run without MERGE) | C03, C05, C08 |
# MAGIC | 1 row pointed at a commodity key that doesn't exist | C09 |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./00_recon_lib

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fact_revenue_txn_defect AS SELECT * FROM fact_revenue_txn;
# MAGIC
# MAGIC -- 1. drop 3 transactions
# MAGIC DELETE FROM fact_revenue_txn_defect
# MAGIC WHERE txn_id IN (SELECT txn_id FROM fact_revenue_txn ORDER BY txn_id LIMIT 3);
# MAGIC
# MAGIC -- 2. one-cent drift on one amount
# MAGIC UPDATE fact_revenue_txn_defect SET revenue_amount = revenue_amount + 0.01
# MAGIC WHERE txn_id = (SELECT max(txn_id) FROM fact_revenue_txn);
# MAGIC
# MAGIC -- 3. duplicate a row under a new id
# MAGIC INSERT INTO fact_revenue_txn_defect
# MAGIC SELECT concat(txn_id, '-dup'), date_key, location_key, commodity_key, revenue_type_key,
# MAGIC        revenue_amount, _src_row_id, _loaded_ts, _updated_ts
# MAGIC FROM fact_revenue_txn ORDER BY txn_id DESC LIMIT 1;
# MAGIC
# MAGIC -- 4. broken foreign key
# MAGIC UPDATE fact_revenue_txn_defect SET commodity_key = -1
# MAGIC WHERE txn_id = (SELECT min(txn_id) FROM fact_revenue_txn WHERE txn_id NOT IN
# MAGIC                 (SELECT txn_id FROM fact_revenue_txn ORDER BY txn_id LIMIT 3));

# COMMAND ----------

defect = run_reconciliation("fact_revenue_txn_defect")
display(defect.select("check_id", "check_name", "source_value", "target_value", "variance", "status"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fact_revenue_txn_defect
