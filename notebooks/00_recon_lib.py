# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Reconciliation library
# MAGIC `run_reconciliation(fact_table)` compares source and target at four levels:
# MAGIC 1. **Row counts**: file → bronze → (silver valid + rejects) → fact
# MAGIC 2. **Control totals**: total revenue, and revenue by fiscal year × revenue type
# MAGIC 3. **Key level**: every valid source transaction is in the target and nothing extra is
# MAGIC 4. **Referential integrity**: every fact row joins to every dimension
# MAGIC
# MAGIC The source side re-reads the raw file rather than the bronze table, so an ingestion error is not
# MAGIC carried into both sides of the comparison.

# COMMAND ----------

import re, uuid
from datetime import datetime
from decimal import Decimal
from pyspark.sql import functions as F

def _snake(c):
    return re.sub(r"[^0-9a-z]+", "_", c.strip().lower()).strip("_")

def run_reconciliation(fact_table="fact_revenue_txn", reject_rate_limit=0.01):
    run_id, run_ts = str(uuid.uuid4())[:8], datetime.now()
    results = []

    def check(cid, name, source, target, tolerance=0):
        src = source if source is not None else 0
        tgt = target if target is not None else 0
        variance = tgt - src
        status = "PASS" if abs(variance) <= tolerance else "FAIL"
        results.append((run_id, run_ts, cid, name, str(src), str(tgt), str(variance), status))

    # Source side: re-read the raw file
    src = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(RAW_FILE)
    src = src.toDF(*[_snake(c) for c in src.columns])
    src.createOrReplaceTempView("v_recon_source")
    s = spark.sql(f"""SELECT count(*) AS n,
                             sum({REVENUE_PARSE_SQL.format(c='revenue')}) AS amt
                      FROM v_recon_source""").first()

    b = spark.sql("SELECT count(*) n FROM bronze_monthly_revenue").first()
    v = spark.sql("SELECT count(*) n, sum(revenue_amount) amt FROM silver_revenue_txn").first()
    r = spark.sql("SELECT count(*) n, coalesce(sum(revenue_amount),0) amt FROM silver_revenue_rejects").first()
    f = spark.sql(f"SELECT count(*) n, sum(revenue_amount) amt FROM {fact_table}").first()

    # 1. row counts
    check("C01", "Row count: source file vs bronze", s.n, b.n)
    check("C02", "Row count: bronze vs silver valid + rejects", b.n, v.n + r.n)
    check("C03", "Row count: silver valid vs fact", v.n, f.n)

    # 2. control totals
    check("C04", "Revenue total: source file vs fact + rejected amounts", s.amt, (f.amt or Decimal(0)) + r.amt)
    check("C05", "Revenue total: silver valid vs fact", v.amt, f.amt)

    bad_groups = spark.sql(f"""
        WITH src AS (
          SELECT CASE WHEN month(revenue_date) >= 10 THEN year(revenue_date)+1 ELSE year(revenue_date) END fy,
                 revenue_type, sum(revenue_amount) amt
          FROM silver_revenue_txn GROUP BY ALL),
        tgt AS (
          SELECT d.fiscal_year fy, rt.revenue_type, sum(f.revenue_amount) amt
          FROM {fact_table} f
          JOIN dim_date d ON f.date_key = d.date_key
          JOIN dim_revenue_type rt ON f.revenue_type_key = rt.revenue_type_key
          GROUP BY ALL)
        SELECT count(*) n FROM src FULL OUTER JOIN tgt USING (fy, revenue_type)
        WHERE coalesce(src.amt, 0) <> coalesce(tgt.amt, 0)""").first().n
    check("C06", "Fiscal year x revenue type groups with a variance", 0, bad_groups)

    # 3. key level
    missing = spark.sql(f"SELECT count(*) n FROM silver_revenue_txn s LEFT ANTI JOIN {fact_table} f USING (txn_id)").first().n
    extra = spark.sql(f"SELECT count(*) n FROM {fact_table} f LEFT ANTI JOIN silver_revenue_txn s USING (txn_id)").first().n
    check("C07", "Source transactions missing from target", 0, missing)
    check("C08", "Target transactions not in source", 0, extra)

    # 4. referential integrity
    orphans = spark.sql(f"""
        SELECT count(*) n FROM {fact_table} f
        LEFT JOIN dim_date d ON f.date_key = d.date_key
        LEFT JOIN dim_location l ON f.location_key = l.location_key
        LEFT JOIN dim_commodity c ON f.commodity_key = c.commodity_key
        LEFT JOIN dim_revenue_type rt ON f.revenue_type_key = rt.revenue_type_key
        WHERE d.date_key IS NULL OR l.location_key IS NULL
           OR c.commodity_key IS NULL OR rt.revenue_type_key IS NULL""").first().n
    check("C09", "Fact rows with a missing dimension", 0, orphans)

    # data quality threshold
    rate = r.n / b.n if b.n else 0
    results.append((run_id, run_ts, "C10", f"Reject rate under {reject_rate_limit:.0%}",
                    "", f"{rate:.4%}", "", "PASS" if rate <= reject_rate_limit else "FAIL"))

    df = spark.createDataFrame(results, "run_id string, run_ts timestamp, check_id string, check_name string, "
                                        "source_value string, target_value string, variance string, status string")
    df = df.withColumn("fact_table", F.lit(fact_table))
    df.write.mode("append").option("mergeSchema", True).saveAsTable("recon_results")
    return df
