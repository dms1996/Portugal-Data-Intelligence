# Portugal Data Intelligence

**Macroeconomic analytics platform for the Portuguese economy (2010-2025)**

## Overview

Portugal Data Intelligence consolidates macroeconomic data from official Portuguese and European authorities, analyses structural trends and cyclical patterns across 15 years, and delivers boardroom-ready reports.

## Key Features

- **6 Economic Pillars** — GDP, Unemployment, Credit, Interest Rates, Inflation, Public Debt
- **Real Data Ingestion** — Eurostat, ECB, and Banco de Portugal APIs with synthetic fallback
- **Star Schema Database** — SQLite with dimension and fact tables, full referential integrity
- **Statistical Analysis** — Correlation, SARIMAX forecasting, STL decomposition, scenario analysis
- **Data Quality Framework** — 15+ automated checks with pipeline lineage tracking
- **Interactive Dashboard** — Power BI dashboard with KPIs, drill-downs, and what-if scenarios
- **Automated Alerts** — Configurable threshold monitoring for all key indicators
- **Executive Reports** — AI-powered insight engine, PowerPoint generation, Power BI DAX measures

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python main.py

# Run specific modes
python main.py --mode etl        # Data fetch + ETL only
python main.py --mode analysis   # Analysis + charts
python main.py --mode reports    # Insights + PowerPoint

# Generate HTML report
python dashboard/generate_report.py
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Database | SQLite 3 (star schema) |
| Analysis | pandas, NumPy, SciPy, statsmodels, scikit-learn |
| Visualisation | matplotlib, seaborn, Plotly |
| AI | OpenAI GPT-4 (optional) |
| Reporting | python-pptx, Power BI DAX |
| Dashboard | Power BI |
| Container | Docker |

## Data Sources

| Source | Authority | Data |
|--------|----------|------|
| Eurostat | European Statistical Office | GDP, Unemployment, Inflation (HICP) |
| ECB | European Central Bank | Interest Rates (Euribor, MRR) |
| Banco de Portugal | BPStat | Credit, Public Debt |

## Project Structure

```
portugal-data-intelligence/
├── config/          # Settings and data source definitions
├── data/            # Raw, processed, and database files
├── dashboard/       # HTML report generator
├── docs/            # Documentation (this site)
├── reports/         # Generated charts, insights, presentations
├── sql/             # DDL and analytical queries
├── src/             # Source code (ETL, analysis, insights, alerts)
└── tests/           # Test suite (360+ tests, ~80% coverage)
```
