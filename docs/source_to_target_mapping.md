# Source-to-target mapping specification

**Source:** ONRR `monthly_revenue.csv` (comma-delimited, header row, all fields text), landed unchanged in
`bronze_monthly_revenue`.
**Target:** `workspace.onrr_migration` star schema: `fact_revenue_txn` with `dim_date`, `dim_location`,
`dim_commodity`, `dim_revenue_type`.
**Grain:** one fact row per source row (month × location × revenue type × commodity × product).

## Field mapping

| # | Source field | Example | Target table.column | Target type | Transformation | Validation |
|---|---|---|---|---|---|---|
| 1 | Date | `01/01/2003` | `fact_revenue_txn.date_key` → `dim_date` | INT (yyyyMMdd) | Parse `MM/dd/yyyy` (also accepts `yyyy-MM-dd`); key = `yyyyMMdd` | V001 not parseable → reject; V005 future date → reject |
| 2 | Land Class | `Federal` | `dim_location.land_class` | STRING | Trim; blank → NULL | V004 missing → reject |
| 3 | Land Category | `Onshore` | `dim_location.land_category` | STRING | Trim; blank → NULL | None |
| 4 | State | `Colorado` | `dim_location.state` | STRING | Trim; blank → NULL (shown as "Not reported" in reporting) | None |
| 5 | County | `Weld` | `dim_location.county` | STRING | Trim; blank → NULL | None |
| 6 | FIPS Code | `08123` | `dim_location.fips_code` | STRING | Trim; kept as text to preserve leading zeros | None |
| 7 | Offshore Region | blank for onshore rows | `dim_location.offshore_region` | STRING | Trim; blank → NULL | None |
| 8 | Revenue Type | `Royalties` | `dim_revenue_type.revenue_type` | STRING | Trim; blank → NULL | V003 missing → reject |
| 9 | Mineral Lease Type | `Oil & Gas` | `dim_commodity.mineral_lease_type` | STRING | Trim; blank → `Not Applicable` | D001 default |
| 10 | Commodity | `Gas` | `dim_commodity.commodity` | STRING | Trim; blank → `Not Applicable` | D001 default |
| 11 | Product | `Processed (Residue) Gas` | `dim_commodity.product` | STRING | Trim; blank → `Not Applicable` | D001 default |
| 12 | Revenue | `23283.03`, `-1941.00`, `(1,941.00)` | `fact_revenue_txn.revenue_amount` | DECIMAL(18,2) | Strip `$`, `,` and spaces; parentheses → negative; cast | V002 not numeric → reject |

## Derived and audit fields

| Target table.column | Rule |
|---|---|
| `fact_revenue_txn.txn_id` | SHA-256 of all source fields + occurrence number among identical rows. Business key for the MERGE and for key-level reconciliation (C07, C08) |
| `fact_revenue_txn._src_row_id` | Row id assigned at landing; traces each fact row to its source row |
| `dim_location.location_key` | `xxhash64(land_class, land_category, state, county, fips_code, offshore_region)` |
| `dim_commodity.commodity_key` | `xxhash64(mineral_lease_type, commodity, product)` |
| `dim_revenue_type.revenue_type_key` | `xxhash64(revenue_type)` |
| `dim_date.fiscal_year` | U.S. federal fiscal year: October to September (month ≥ 10 → year + 1) |
| `bronze_monthly_revenue._row_hash` | SHA-256 of the source row; used for duplicate flag W001 |

## Load rules

| Rule | Detail |
|---|---|
| Rejects | Rows failing any V-rule go to `silver_revenue_rejects` with reason codes; never dropped |
| Duplicates | Identical source rows are flagged (W001) and loaded, since aggregated data can legitimately repeat |
| Fact load | `MERGE` on `txn_id`: insert new, update changed amounts, delete rows no longer in source |
| Dimension load | `MERGE` on surrogate key, insert only |
| Sign-off | Checks C01–C10 must all pass (notebook 04); C11 ties out to ONRR's published fiscal-year totals (notebook 07) |
