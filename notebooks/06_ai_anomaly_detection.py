# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · AI: flag unusual monthly revenue for review
# MAGIC Reconciliation proves the data moved correctly. This notebook asks a different question:
# MAGIC **does any of the migrated data look wrong in a business sense?** (a royalty month that is 10x
# MAGIC normal, a sudden collapse, a sign flip). Those are the rows a finance analyst would want to see first.
# MAGIC
# MAGIC Method, per commodity × revenue type monthly series:
# MAGIC * robust z-score against the trailing 12-month median (resistant to outliers)
# MAGIC * month-over-month change
# MAGIC * an Isolation Forest over those features; a month is flagged when the model and the z-score agree

# COMMAND ----------

# MAGIC %pip install -q scikit-learn

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

pdf = spark.sql("""
    SELECT d.month_start, c.commodity, rt.revenue_type,
           CAST(sum(f.revenue_amount) AS DOUBLE) AS revenue
    FROM fact_revenue_txn f
    JOIN dim_date d          ON f.date_key = d.date_key
    JOIN dim_commodity c     ON f.commodity_key = c.commodity_key
    JOIN dim_revenue_type rt ON f.revenue_type_key = rt.revenue_type_key
    GROUP BY ALL
""").toPandas()

# keep the 15 largest series with at least 3 years of history
size = pdf.groupby(["commodity", "revenue_type"]).agg(months=("revenue", "size"),
                                                      total=("revenue", lambda s: s.abs().sum()))
keep = size[size.months >= 36].nlargest(15, "total").index
pdf = pdf.set_index(["commodity", "revenue_type"]).loc[keep].reset_index()
pdf["month_start"] = pd.to_datetime(pdf["month_start"])
pdf = pdf.sort_values(["commodity", "revenue_type", "month_start"])

def features(g):
    g = g.copy()
    slog = np.sign(g.revenue) * np.log1p(g.revenue.abs())
    med = slog.shift(1).rolling(12, min_periods=6).median()
    mad = (slog.shift(1) - med).abs().rolling(12, min_periods=6).median()
    g["robust_z"] = (slog - med) / (1.4826 * mad + 1e-6)
    g["mom_change"] = slog.diff()
    return g

pdf = pd.concat([features(g) for _, g in pdf.groupby(["commodity", "revenue_type"])]).dropna()
X = pdf[["robust_z", "mom_change"]].clip(-50, 50)
model = IsolationForest(n_estimators=200, contamination=0.02, random_state=42).fit(X)
pdf["iforest_score"] = -model.score_samples(X)
pdf["is_anomaly"] = (model.predict(X) == -1) & (pdf.robust_z.abs() > 3)

anomalies = pdf[pdf.is_anomaly].sort_values("iforest_score", ascending=False)
print(f"{len(anomalies)} anomalous months out of {len(pdf):,} series-months")

(spark.createDataFrame(pdf)
      .write.mode("overwrite").option("overwriteSchema", True)
      .saveAsTable("gold_revenue_anomalies"))

display(anomalies.head(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Plot this as a line chart (month_start vs revenue), colour by is_anomaly
# MAGIC SELECT month_start, revenue, is_anomaly
# MAGIC FROM gold_revenue_anomalies
# MAGIC WHERE commodity = 'Oil' AND revenue_type = 'Royalties'
# MAGIC ORDER BY month_start
