# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Tie-out to ONRR published fiscal-year totals
# MAGIC Notebook 04 proves the target matches the extract. This notebook checks the migrated data against a
# MAGIC second, independent source: the fiscal-year revenue file ONRR publishes separately
# MAGIC (`fiscal_year_revenue.csv`). This mirrors a finance sign-off, where migrated balances are tied out to
# MAGIC the legacy system's own reports.
# MAGIC
# MAGIC Scope: fiscal years 2004 to 2025. The monthly extract starts in January 2003 and ends in August 2026,
# MAGIC so FY2003 and FY2026 are partial years and are excluded.
# MAGIC
# MAGIC Comparison grain: fiscal year x land class x revenue type. A group passes when the absolute variance
# MAGIC is $1.00 or less (allows for rounding in the published file).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os, re, urllib.request
from pyspark.sql import functions as F

FY_FILE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/fiscal_year_revenue.csv"
FY_URL = "https://revenuedata.onrr.gov/downloads/fiscal_year_revenue.csv"
FIRST_FY, LAST_FY, TOLERANCE = 2004, 2025, 1.00

if not os.path.exists(FY_FILE):
    try:
        urllib.request.urlretrieve(FY_URL, FY_FILE)
        print("Downloaded fiscal_year_revenue.csv from ONRR")
    except Exception as e:
        raise Exception(f"Download failed ({e}). Upload fiscal_year_revenue.csv to {FY_FILE} manually.")
else:
    print("Published file already in volume")

def snake(c):
    return re.sub(r"[^0-9a-z]+", "_", c.strip().lower()).strip("_")

pub = spark.read.option("header", True).option("escape", '"').csv(FY_FILE)
pub = pub.toDF(*[snake(c) for c in pub.columns])
pub.createOrReplaceTempView("v_published_fy")
print("Published rows:", pub.count(), "| columns:", pub.columns)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE tieout_fy_published AS
WITH published AS (
  SELECT CAST(fiscal_year AS INT) AS fiscal_year,
         trim(land_class) AS land_class,
         trim(revenue_type) AS revenue_type,
         sum({REVENUE_PARSE_SQL.format(c='revenue')}) AS published_amount
  FROM v_published_fy
  WHERE CAST(fiscal_year AS INT) BETWEEN {FIRST_FY} AND {LAST_FY}
  GROUP BY ALL),
migrated AS (
  SELECT d.fiscal_year, l.land_class, rt.revenue_type, sum(f.revenue_amount) AS migrated_amount
  FROM fact_revenue_txn f
  JOIN dim_date d          ON f.date_key = d.date_key
  JOIN dim_location l      ON f.location_key = l.location_key
  JOIN dim_revenue_type rt ON f.revenue_type_key = rt.revenue_type_key
  WHERE d.fiscal_year BETWEEN {FIRST_FY} AND {LAST_FY}
  GROUP BY ALL)
SELECT fiscal_year, land_class, revenue_type,
       coalesce(published_amount, 0) AS published_amount,
       coalesce(migrated_amount, 0)  AS migrated_amount,
       coalesce(migrated_amount, 0) - coalesce(published_amount, 0) AS variance,
       abs(coalesce(migrated_amount, 0) - coalesce(published_amount, 0)) <= {TOLERANCE} AS matched
FROM published FULL OUTER JOIN migrated USING (fiscal_year, land_class, revenue_type)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary
# MAGIC SELECT count(*)                       AS groups_compared,
# MAGIC        count_if(matched)              AS groups_matched,
# MAGIC        count_if(NOT matched)          AS groups_with_variance,
# MAGIC        sum(published_amount)          AS published_total,
# MAGIC        sum(migrated_amount)           AS migrated_total,
# MAGIC        sum(migrated_amount) - sum(published_amount) AS net_variance
# MAGIC FROM tieout_fy_published

# COMMAND ----------

# MAGIC %sql
# MAGIC -- By fiscal year
# MAGIC SELECT fiscal_year,
# MAGIC        sum(published_amount) AS published_amount,
# MAGIC        sum(migrated_amount)  AS migrated_amount,
# MAGIC        sum(variance)         AS variance,
# MAGIC        count_if(NOT matched) AS groups_with_variance
# MAGIC FROM tieout_fy_published
# MAGIC GROUP BY fiscal_year ORDER BY fiscal_year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Groups that do not tie out (investigate these)
# MAGIC SELECT * FROM tieout_fy_published WHERE NOT matched ORDER BY abs(variance) DESC

# COMMAND ----------

# Record the result as C11 under the latest sign-off run, so it appears with C01-C10
latest = spark.sql("""SELECT max_by(run_id, run_ts) AS run_id, max(run_ts) AS run_ts FROM recon_results
                      WHERE fact_table = 'fact_revenue_txn'""").first()
spark.sql(f"DELETE FROM recon_results WHERE run_id = '{latest.run_id}' AND check_id = 'C11'")
s = spark.sql("""SELECT count(*) n, count_if(matched) m, sum(published_amount) p, sum(migrated_amount) t
                 FROM tieout_fy_published""").first()
row = [(latest.run_id, latest.run_ts, "C11",
        f"Tie-out to ONRR published FY totals, FY{FIRST_FY}-FY{LAST_FY} (groups matched)",
        str(s.n), str(s.m), str(s.m - s.n), "PASS" if s.m == s.n else "FAIL", "fact_revenue_txn")]
(spark.createDataFrame(row, "run_id string, run_ts timestamp, check_id string, check_name string, "
                            "source_value string, target_value string, variance string, status string, fact_table string")
      .write.mode("append").saveAsTable("recon_results"))
print(f"{s.m} of {s.n} groups tie out | published {s.p:,.2f} | migrated {s.t:,.2f} | net {s.t - s.p:,.2f}")
