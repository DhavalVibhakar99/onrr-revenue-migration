-- Catalog/schema : workspace.onrr_migration

-- 1. Migration sign-off: latest reconciliation run
SELECT check_id, check_name, source_value, target_value, variance, status
FROM recon_results
WHERE fact_table = 'fact_revenue_txn'
  AND run_id = (SELECT max_by(run_id, run_ts) FROM recon_results WHERE fact_table = 'fact_revenue_txn')
ORDER BY check_id;

-- 2. Revenue by fiscal year and revenue type
SELECT d.fiscal_year, rt.revenue_type, sum(f.revenue_amount) AS revenue
FROM fact_revenue_txn f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_revenue_type rt ON f.revenue_type_key = rt.revenue_type_key
GROUP BY ALL ORDER BY d.fiscal_year;

-- 3. Oil & gas royalties by state, onshore only
SELECT l.state, c.commodity, sum(f.revenue_amount) AS royalties
FROM fact_revenue_txn f
JOIN dim_location l ON f.location_key = l.location_key
JOIN dim_commodity c ON f.commodity_key = c.commodity_key
JOIN dim_revenue_type rt ON f.revenue_type_key = rt.revenue_type_key
WHERE rt.revenue_type = 'Royalties' AND c.commodity IN ('Oil', 'Gas') AND l.land_category = 'Onshore'
GROUP BY ALL ORDER BY royalties DESC;

-- 4. Rejected rows by reason
SELECT reason, count(*) AS rows
FROM silver_revenue_rejects LATERAL VIEW explode(reject_reasons) t AS reason
GROUP BY reason;

-- 5. AI-flagged months for analyst review
SELECT month_start, commodity, revenue_type, revenue, round(robust_z, 1) AS robust_z
FROM gold_revenue_anomalies
WHERE is_anomaly
ORDER BY iforest_score DESC;
