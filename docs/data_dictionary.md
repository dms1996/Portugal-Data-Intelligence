# Data Dictionary

## Portugal Data Intelligence - Complete Data Dictionary

**Version:** 1.0
**Last Updated:** March 2026

---

## Overview

This document defines every table, column, data type, and constraint in the Portugal
Data Intelligence database. The schema follows a star schema design with shared dimension
tables and six pillar-specific fact tables.

---

## Numeric Conventions

All downstream consumers (including Power BI DAX measures) should observe these conventions:

- **Percentages** are stored as actual percentage values -- e.g., `6.2` means 6.2%, **not** 0.062.
- **Monetary values** are denominated in **EUR millions** unless otherwise noted in the column description.
- **Rates** (interest rates, growth rates) are expressed in **percentage points** -- e.g., `1.50` means 1.50 pp.
- Columns whose unit is `bps` (basis points) are an exception: `150` means 150 basis points (= 1.50 pp).

---

## Dimension Tables

### dim_date

The calendar dimension table, providing temporal attributes for all fact table joins.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `date_key` | INTEGER | No (PK) | Surrogate key in YYYYMMDD format |
| `full_date` | DATE | No | Full date value (YYYY-MM-DD) |
| `year` | INTEGER | No | Calendar year (2010-2025) |
| `quarter` | INTEGER | No | Calendar quarter (1-4) |
| `quarter_label` | TEXT | No | Quarter label (e.g., "2010-Q1") |
| `month` | INTEGER | No | Calendar month (1-12) |
| `month_name` | TEXT | No | Full month name (e.g., "January") |
| `month_short` | TEXT | No | Abbreviated month name (e.g., "Jan") |
| `year_month` | TEXT | No | Year-month label (e.g., "2010-01") |
| `is_quarter_end` | INTEGER | No | Flag: 1 if last month of quarter, else 0 |
| `fiscal_year` | INTEGER | No | Portuguese fiscal year (aligned with calendar year) |

**Granularity:** One row per month from January 2010 to December 2025 (192 rows).

---

### dim_source

Reference table for data sources and their metadata.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `source_key` | INTEGER | No (PK) | Auto-increment surrogate key |
| `source_code` | TEXT | No (UQ) | Short code (e.g., "INE", "BDP", "PORDATA", "EUROSTAT") |
| `source_name` | TEXT | No | Full name of the data source |
| `source_url` | TEXT | No | Official website URL |
| `description` | TEXT | Yes | Brief description of the source |
| `country` | TEXT | No | Country of origin (e.g., "PT", "EU") |

**Granularity:** One row per data source (approximately 5 rows).

---

## Fact Tables

### Pillar 1: fact_gdp

**Granularity:** Quarterly (2010-Q1 to 2025-Q4 = 64 rows)
**Primary Sources:** INE, Eurostat

| Column | Data Type | Nullable | Unit | Description |
|--------|-----------|----------|------|-------------|
| `gdp_key` | INTEGER | No (PK) | - | Auto-increment surrogate key |
| `date_key` | INTEGER | No (FK) | - | Foreign key to dim_date (quarter-end month) |
| `source_key` | INTEGER | No (FK) | - | Foreign key to dim_source |
| `nominal_gdp` | REAL | No | EUR millions | Gross Domestic Product at current prices |
| `real_gdp` | REAL | No | EUR millions | GDP at constant prices (base year 2015) |
| `gdp_growth_qoq` | REAL | Yes | % | Quarter-on-quarter GDP growth rate |
| `gdp_growth_yoy` | REAL | Yes | % | Year-on-year GDP growth rate |
| `gdp_per_capita` | REAL | Yes | EUR | GDP per capita (annualised nominal GDP / population). Note: `nominal_gdp` is quarterly, but `gdp_per_capita` is always annualised per standard convention. |
| `gdp_deflator` | REAL | Yes | index | GDP deflator (base year 2015 = 100) |

**Key relationships:**
- `date_key` references `dim_date.date_key`
- `source_key` references `dim_source.source_key`

---

### Pillar 2: fact_unemployment

**Granularity:** Monthly (Jan 2010 to Dec 2025 = 192 rows)
**Primary Sources:** INE, Eurostat

| Column | Data Type | Nullable | Unit | Description |
|--------|-----------|----------|------|-------------|
| `unemployment_key` | INTEGER | No (PK) | - | Auto-increment surrogate key |
| `date_key` | INTEGER | No (FK) | - | Foreign key to dim_date |
| `source_key` | INTEGER | No (FK) | - | Foreign key to dim_source |
| `unemployment_rate` | REAL | No | % | Total unemployment rate (ILO definition) |
| `youth_unemployment_rate` | REAL | Yes | % | Unemployment rate for ages 15-24 |
| `long_term_unemployment_rate` | REAL | Yes | % | Unemployment > 12 months as % of active population |
| `labour_force_participation` | REAL | Yes | % | Labour force participation rate (15-64) |
| `employment_rate` | REAL | Yes | % | Employment rate (15-64) |
| `unemployed_thousands` | REAL | Yes | thousands | Absolute number of unemployed persons |

**Key relationships:**
- `date_key` references `dim_date.date_key`
- `source_key` references `dim_source.source_key`

---

### Pillar 3: fact_credit

**Granularity:** Monthly (Jan 2010 to Dec 2025 = 192 rows)
**Primary Sources:** Banco de Portugal

| Column | Data Type | Nullable | Unit | Description |
|--------|-----------|----------|------|-------------|
| `credit_key` | INTEGER | No (PK) | - | Auto-increment surrogate key |
| `date_key` | INTEGER | No (FK) | - | Foreign key to dim_date |
| `source_key` | INTEGER | No (FK) | - | Foreign key to dim_source |
| `total_credit` | REAL | No | EUR millions | Total credit to the economy (outstanding stock) |
| `credit_nfc` | REAL | Yes | EUR millions | Credit to non-financial corporations |
| `credit_households` | REAL | Yes | EUR millions | Credit to households |
| `credit_housing` | REAL | Yes | EUR millions | Housing credit (mortgage lending) |
| `credit_consumption` | REAL | Yes | EUR millions | Consumer credit |
| `npl_ratio` | REAL | Yes | % | Non-performing loan ratio |
| `credit_growth_yoy` | REAL | Yes | % | Year-on-year growth in total credit |

**Key relationships:**
- `date_key` references `dim_date.date_key`
- `source_key` references `dim_source.source_key`

---

### Pillar 4: fact_interest_rates

**Granularity:** Monthly (Jan 2010 to Dec 2025 = 192 rows)
**Primary Sources:** Banco de Portugal, ECB

| Column | Data Type | Nullable | Unit | Description |
|--------|-----------|----------|------|-------------|
| `rate_key` | INTEGER | No (PK) | - | Auto-increment surrogate key |
| `date_key` | INTEGER | No (FK) | - | Foreign key to dim_date |
| `source_key` | INTEGER | No (FK) | - | Foreign key to dim_source |
| `ecb_main_refinancing_rate` | REAL | No | % | ECB main refinancing operations rate |
| `ecb_deposit_facility_rate` | REAL | Yes | % | ECB deposit facility rate |
| `euribor_3m` | REAL | Yes | % | 3-month Euribor rate (monthly average) |
| `euribor_6m` | REAL | Yes | % | 6-month Euribor rate (monthly average) |
| `euribor_12m` | REAL | Yes | % | 12-month Euribor rate (monthly average) |
| `pt_bond_yield_10y` | REAL | Yes | % | Portuguese 10-year government bond yield |
| `de_bond_yield_10y` | REAL | Yes | % | German 10-year Bund yield (benchmark) |
| `spread_pt_de` | REAL | Yes | bps | Spread between PT and DE 10-year yields |

**Key relationships:**
- `date_key` references `dim_date.date_key`
- `source_key` references `dim_source.source_key`

---

### Pillar 5: fact_inflation

**Granularity:** Monthly (Jan 2010 to Dec 2025 = 192 rows)
**Primary Sources:** INE, Eurostat

| Column | Data Type | Nullable | Unit | Description |
|--------|-----------|----------|------|-------------|
| `inflation_key` | INTEGER | No (PK) | - | Auto-increment surrogate key |
| `date_key` | INTEGER | No (FK) | - | Foreign key to dim_date |
| `source_key` | INTEGER | No (FK) | - | Foreign key to dim_source |
| `hicp_index` | REAL | No | index | Harmonised Index of Consumer Prices (2015=100) |
| `hicp_yoy` | REAL | Yes | % | HICP year-on-year change |
| `hicp_mom` | REAL | Yes | % | HICP month-on-month change |
| `cpi_index` | REAL | Yes | index | National Consumer Price Index (2015=100) |
| `cpi_yoy` | REAL | Yes | % | CPI year-on-year change |
| `core_inflation_yoy` | REAL | Yes | % | Core inflation (excluding energy and food) YoY |
| `energy_inflation_yoy` | REAL | Yes | % | Energy component inflation YoY |
| `food_inflation_yoy` | REAL | Yes | % | Food component inflation YoY |

**Key relationships:**
- `date_key` references `dim_date.date_key`
- `source_key` references `dim_source.source_key`

---

### Pillar 6: fact_public_debt

**Granularity:** Quarterly (2010-Q1 to 2025-Q4 = 64 rows)
**Primary Sources:** Banco de Portugal, PORDATA

| Column | Data Type | Nullable | Unit | Description |
|--------|-----------|----------|------|-------------|
| `debt_key` | INTEGER | No (PK) | - | Auto-increment surrogate key |
| `date_key` | INTEGER | No (FK) | - | Foreign key to dim_date (quarter-end month) |
| `source_key` | INTEGER | No (FK) | - | Foreign key to dim_source |
| `total_debt` | REAL | No | EUR millions | General government consolidated gross debt |
| `debt_to_gdp` | REAL | No | % | Debt-to-GDP ratio (Maastricht definition) |
| `external_debt` | REAL | Yes | EUR millions | Debt held by non-resident investors |
| `domestic_debt` | REAL | Yes | EUR millions | Debt held by resident investors |
| `short_term_debt` | REAL | Yes | EUR millions | Debt with maturity < 1 year |
| `long_term_debt` | REAL | Yes | EUR millions | Debt with maturity >= 1 year |
| `debt_change_qoq` | REAL | Yes | EUR millions | Quarter-on-quarter change in total debt |

**Key relationships:**
- `date_key` references `dim_date.date_key`
- `source_key` references `dim_source.source_key`

---

## Source References

| Source Code | Full Name | URL | Data Used |
|-------------|----------|-----|-----------|
| INE | Instituto Nacional de Estatistica | https://www.ine.pt | GDP, Unemployment, Inflation (CPI) |
| BDP | Banco de Portugal (BPStat) | https://bpstat.bportugal.pt | Credit, Interest Rates, Public Debt |
| PORDATA | Base de Dados Portugal Contemporaneo | https://www.pordata.pt | Public Debt, cross-reference data |
| EUROSTAT | European Statistical Office | https://ec.europa.eu/eurostat | GDP, Unemployment, Inflation (HICP) |
| ECB | European Central Bank | https://www.ecb.europa.eu/stats | Interest Rates (ECB rates, Euribor) |

---

## Data Quality Rules

| Rule | Scope | Description |
|------|-------|-------------|
| Not null | All primary metrics | Core indicators must not contain null values |
| Range check | Rates and percentages | Unemployment, inflation, and interest rates must be within plausible bounds |
| Temporal completeness | All fact tables | No gaps in the time series within the defined period |
| Referential integrity | All foreign keys | Every `date_key` and `source_key` must exist in the respective dimension table |
| Type enforcement | All columns | Strict data type validation during ETL load stage |
