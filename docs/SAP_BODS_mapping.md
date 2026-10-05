# How this pipeline maps to SAP BODS

SAP BusinessObjects Data Services (BODS) is licensed software with no free edition, so this project
is built in Databricks. Every step follows a standard BODS pattern, mapped below.

## BODS building blocks

| BODS term | What it is | Where it is in this project |
|---|---|---|
| **Datastore** | A saved connection to a source or target system (SAP ECC, Oracle, flat file) | The `raw` volume (source) and the `onrr_migration` schema (target) |
| **File format** | Definition of a flat file's columns and delimiters | The CSV read options in `01_bronze_ingest` |
| **Project → Job → Workflow → Dataflow** | Hierarchy: a job runs workflows, which run dataflows in order; a dataflow is one source-to-target movement | The whole repo = project; running 01→04 in order = job; each notebook ≈ a dataflow |
| **Global variables / substitution parameters** | Job-wide settings changed per environment (DEV/QA/PROD) | `00_config` |
| **Query transform** | Select, join, filter, map, derive columns  | `v_typed` view in `02_silver_validate` |
| **Validation transform** | Applies rules per row, sends rows to **Pass** and **Fail** outputs | `v_validated` → `silver_revenue_txn` / `silver_revenue_rejects` |
| **Data Quality transforms** (Data Cleanse, Match) | Standardise values, find duplicates | Trimming, defaulting to 'Not Applicable', `w001_duplicate_flag` |
| **Key_Generation** | Creates surrogate keys for dimension rows | `xxhash64(...)` keys in `03_gold_load` |
| **Table_Comparison** | Compares incoming rows to the target and tags each INSERT / UPDATE / DELETE | The `MERGE ... WHEN MATCHED / NOT MATCHED / NOT MATCHED BY SOURCE` |
| **Map_Operation** | Changes the operation code of rows (e.g. turn UPDATE into INSERT) | Implicit in the MERGE clauses |
| **History_Preserving** | Builds SCD Type 2 history (valid_from / valid_to) | Not needed here; Delta `DESCRIBE HISTORY` gives load-level history |
| **Auditing (row counts on ports)** + **Try/Catch** | BODS audit points compare counts/sums between source and target; Try/Catch handles failures | `00_recon_lib` + the `raise Exception` gate in `04_reconciliation` |
| **Recovery / restartability** | Re-run a failed job without duplicating loads | Idempotent MERGE: re-running produces no changes |

## A typical SAP finance migration in BODS

In an S/4HANA migration, BODS usually runs this cycle for each object (GL balances, open AP/AR items,
vendors, customers, assets):

1. **Extract** from the legacy system (often SAP ECC or a non-SAP ERP) into a staging database.
2. **Transform** with Query transforms: map legacy codes to new ones using lookup tables
   (old cost centre → new cost centre, old GL account → new GL account). The BODS function for this is `lookup_ext()`.
3. **Validate** against S/4 config: does the company code exist, is the posting period open, does the GL account exist.
   Failures go to an error table the business fixes.
4. **Load** into S/4 through IDocs, BAPIs, or the SAP Migration Cockpit staging tables.
5. **Reconcile**: trial balance before vs after, open item counts and totals per company code. Finance signs off.

Steps 3 and 5 correspond to notebooks 02 and 04.
