"""Integration tests for the ETL pipeline orchestrator (src/etl/pipeline.py).

These tests require the production database and raw data files to exist.
They are skipped automatically when those artefacts are not available
(e.g. in CI before the first full pipeline run).
"""

import pytest
from pathlib import Path

from config.settings import DATABASE_PATH, RAW_DATA_DIR
from tests.conftest import PRODUCTION_DB

# Skip the entire module if the production database or raw data is missing.
pytestmark = pytest.mark.skipif(
    not PRODUCTION_DB.exists(),
    reason="Production database not available — run 'python main.py' first",
)

EXPECTED_PILLARS = {
    "gdp", "unemployment", "credit",
    "interest_rates", "inflation", "public_debt",
}


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

class TestRunExtract:
    """run_extract returns raw DataFrames for all six pillars."""

    @pytest.fixture(scope="class")
    def raw_data(self):
        from src.etl.pipeline import run_extract
        return run_extract()

    def test_returns_dict(self, raw_data):
        assert isinstance(raw_data, dict)

    def test_six_pillars(self, raw_data):
        assert set(raw_data.keys()) == EXPECTED_PILLARS

    def test_dataframes_non_empty(self, raw_data):
        for key, df in raw_data.items():
            assert len(df) > 0, f"Pillar '{key}' DataFrame is empty"

    def test_gdp_has_date_column(self, raw_data):
        assert "date" in raw_data["gdp"].columns


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

class TestRunTransform:
    """run_transform produces processed DataFrames with date_key columns."""

    @pytest.fixture(scope="class")
    def processed_data(self):
        from src.etl.pipeline import run_extract, run_transform
        raw = run_extract()
        return run_transform(raw)

    def test_returns_dict(self, processed_data):
        assert isinstance(processed_data, dict)

    def test_six_pillars(self, processed_data):
        assert set(processed_data.keys()) == EXPECTED_PILLARS

    def test_date_key_present(self, processed_data):
        for key, df in processed_data.items():
            assert "date_key" in df.columns, f"'{key}' missing date_key"

    def test_gdp_columns(self, processed_data):
        expected = {
            "date_key", "nominal_gdp", "real_gdp",
            "gdp_growth_yoy", "gdp_growth_qoq", "gdp_per_capita",
        }
        assert expected.issubset(set(processed_data["gdp"].columns))

    def test_unemployment_columns(self, processed_data):
        expected = {
            "date_key", "unemployment_rate", "youth_unemployment_rate",
        }
        assert expected.issubset(set(processed_data["unemployment"].columns))

    def test_inflation_columns(self, processed_data):
        expected = {"date_key", "hicp", "cpi", "core_inflation"}
        assert expected.issubset(set(processed_data["inflation"].columns))

    def test_credit_columns(self, processed_data):
        expected = {"date_key", "total_credit", "npl_ratio"}
        assert expected.issubset(set(processed_data["credit"].columns))


# ---------------------------------------------------------------------------
# Summary helper
# ---------------------------------------------------------------------------

class TestPrintSummary:
    """The summary printer should not crash with valid data."""

    def test_summary_does_not_raise(self):
        from src.etl.pipeline import _print_summary
        _print_summary(
            raw_counts={"gdp": 64, "unemployment": 192},
            processed_counts={"gdp": 64, "unemployment": 192},
            loaded_counts={"gdp": 64, "unemployment": 192},
            elapsed=1.23,
        )
