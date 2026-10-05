# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Source-to-target reconciliation (the sign-off gate)
# MAGIC Runs every check and writes the results to `recon_results`. If any check fails, the notebook
# MAGIC raises an error, so a scheduled job stops and nothing downstream uses unreconciled data.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./00_recon_lib

# COMMAND ----------

results = run_reconciliation("fact_revenue_txn")
display(results.select("check_id", "check_name", "source_value", "target_value", "variance", "status"))

# COMMAND ----------

failed = results.filter("status = 'FAIL'").count()
if failed:
    raise Exception(f"Reconciliation FAILED: {failed} check(s). Migration not signed off.")
print("All reconciliation checks passed. Migration signed off.")
