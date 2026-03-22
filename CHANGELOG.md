# Changelog

All notable changes to Portugal Data Intelligence are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [2.0.0] - 2026-03-18

### Added
- **Data Lineage & Batch Tracking** — `PipelineTracker` context manager with UUID-based `run_id`, `pipeline_runs` and `data_lineage` tables for full traceability (`src/etl/lineage.py`)
- **Data Quality Framework** — `DataQualityChecker` with 15+ named checks (schema, ranges, completeness, consistency, freshness) and JSON reports (`src/etl/data_quality.py`)
- **Structured JSON Logging** — `JsonFormatter` with `contextvars`-based correlation IDs; opt-in via `LOG_FORMAT_JSON` env var (`src/utils/logger.py`)
- **File Checksums & Provenance** — SHA-256 sidecar files and `.meta.json` for each raw CSV; checksum verification on extract (`src/etl/fetch_real_data.py`, `src/etl/extract.py`)
- **SARIMAX Forecasting** — Automatic order selection via AIC with Ljung-Box residual diagnostics; graceful fallback to existing methods (`src/analysis/forecasting.py`)
- **STL Decomposition** — Seasonal-trend decomposition for unemployment, inflation, and GDP with 3-panel diagnostic charts (`src/analysis/decomposition.py`)
- **Forecast Backtesting** — Expanding-window cross-validation with MAE, RMSE, MAPE, and directional accuracy (`src/analysis/backtesting.py`)
- **Scenario Calibration** — Data-driven coefficient estimation (Okun's law, credit-rate elasticity) with documented academic fallback sources (`src/analysis/scenario_analysis.py`)
- **Docker Support** — `Dockerfile` and `docker-compose.yml` for reproducible pipeline execution
- **Alert Engine** — Configurable threshold monitoring with warning/critical severity levels and JSON output (`src/alerts/alert_engine.py`, `config/alert_thresholds.json`)
- **New test suites** — `test_lineage.py`, `test_data_quality_framework.py`, `test_forecasting.py`, `test_scenario.py`, `test_backtesting.py`, `test_decomposition.py`, `test_alerts.py`, `test_load.py`, `test_logger.py`

### Changed
- `requirements.txt` — Added `statsmodels>=0.14.0`
- `pyproject.toml` — Version bumped to 2.0.0; coverage threshold raised from 20% to 28%
- `config/settings.py` — Added `DATA_RANGES`, `DQ_FAIL_ON_ERROR`, `DATA_QUALITY_DIR`
- `src/etl/pipeline.py` — Integrated lineage tracking and DQ quality gate between transform and load
- `src/ai_insights/insight_engine.py` — Reduced from 1979 to 605 lines via modular extraction
- `src/reporting/generate_presentation.py` — Reduced from 1772 to 150 lines; slide logic moved to `slides/` package

### Refactored
- `src/ai_insights/insight_engine.py` (1979 → 621 lines) split into:
  - `pillar_insights.py` — Per-pillar rule-based narrative logic
  - `cross_pillar_insights.py` — Cross-pillar relationship analysis
  - `ai_narrator.py` — GPT-4 integration
- `src/reporting/generate_presentation.py` (1772 → 150 lines) split into:
  - `slides/helpers.py` — Shared formatting utilities
  - `slides/data_fetcher.py` — Database query layer
  - `slides/opening.py` — Title, agenda, executive summary slides
  - `slides/economic_pillars.py` — GDP, unemployment, credit, rates, inflation, debt slides
  - `slides/cross_pillar.py` — Correlation, Phillips curve, benchmarking, risk matrix slides

### Metrics
- Test count: 119 → 206 (+73%)
- Test coverage: 23% → 32%
- Max file size: 1979 → 1045 lines (pillar_insights.py)

## [1.0.0] - 2026-03-17

### Added
- Initial release with full ETL pipeline (extract, transform, load)
- Star schema SQLite database with 6 fact tables and 2 dimension tables
- Real data fetching from Eurostat, ECB, and Banco de Portugal APIs
- Synthetic data generation fallback
- Statistical analysis: correlation, benchmarking, significance tests
- AI-powered insight engine (rule-based + GPT-4)
- PowerPoint presentation generator
- Power BI DAX measures (39 measures) and dashboard specification
- 7 analytical SQL query files
- Jupyter exploratory notebook
- 119 tests across 7 test files
