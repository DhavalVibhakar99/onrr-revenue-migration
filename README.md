# Oil & Gas Revenue Migration with Reconciliation (Databricks)

Migrates 20+ years of U.S. federal oil, gas and mineral revenue transactions (royalties, rents,
bonuses, penalties) from a legacy flat-file extract into a Delta Lake star schema on Databricks,
with rule-based validation, an idempotent MERGE load, automated source-to-target reconciliation,
and AI anomaly detection on the migrated data.

**Data:** [ONRR Monthly Revenue](https://revenuedata.doi.gov/downloads/revenue/), U.S. Department of
the Interior, Office of Natural Resources Revenue. Public, real, Jan 2003 to present.

## Architecture

```
monthly_revenue.csv (legacy extract)
        │  01 land as-is + audit columns (row hash, source row id)
        ▼
bronze_monthly_revenue  ── all strings, nothing changed
        │  02 type, cleanse, validate (reason codes V001–V005)
        ├──────────────► silver_revenue_rejects   (failed rows + reasons)
        ▼
silver_revenue_txn
        │  03 hash surrogate keys + MERGE (insert / update / delete)
        ▼
fact_revenue_txn ── dim_date (fiscal year) · dim_location · dim_commodity · dim_revenue_type
        │
        ├── 04 reconciliation gate → recon_results (fails the job on any variance)
        ├── 05 defect injection demo (proves the checks catch real errors)
        └── 06 AI anomaly detection → gold_revenue_anomalies
```

## Reconciliation checks

| ID | Check |
|---|---|
| C01 | Row count: source file vs bronze (independent re-read of the file) |
| C02 | Row count: bronze = silver valid + rejects (nothing silently dropped) |
| C03 | Row count: silver valid vs fact |
| C04 | Revenue total: source file vs fact + rejected amounts |
| C05 | Revenue total: silver valid vs fact |
| C06 | Revenue by fiscal year × revenue type: zero groups with a variance |
| C07 / C08 | Key level: no source transaction missing, no extra target transaction |
| C09 | Referential integrity: every fact row joins to every dimension |
| C10 | Reject rate under 1% |

## Run it (Databricks Free Edition)

1. Sign up for **Databricks Free Edition** (free, no credit card).
2. Push this folder to a GitHub repo. In Databricks: **Workspace → Create → Git folder**, paste the repo URL.
   (Or import each file in `notebooks/` via **Workspace → Import**.)
3. Open `01_bronze_ingest` and run the first two cells. The first creates the schema and volume; the
   second tries to download the data. If it says the download failed: download
   `monthly_revenue.csv` from the ONRR link above in your browser, then in Databricks go to
   **Catalog → workspace → onrr_migration → raw → Upload to this volume**.
4. Run notebooks `01` → `02` → `03` → `04` → `05` → `06` in order.
5. Build a dashboard from `sql/dashboard_queries.sql` (**New → Dashboard**). Screenshot it and the
   `04` results for the README.
6. Optional: **Jobs & Pipelines → Create job** with tasks 01→02→03→04 chained, to show orchestration.

## SAP BODS

See [`docs/SAP_BODS_mapping.md`](docs/SAP_BODS_mapping.md) for how every step maps to BODS transforms.
