"""Tests for the data quality framework."""

import pytest
import pandas as pd
import numpy as np

from src.etl.data_quality import DataQualityChecker, DQReport, CheckResult


def _make_pillar_df(pillar: str) -> pd.DataFrame:
    """Create a minimal valid DataFrame for a given pillar."""
    if pillar == "gdp":
        keys = [f"2010-Q{q}" for q in range(1, 5)] * 16
        return pd.DataFrame({
            "date_key": keys[:64],
            "nominal_gdp": np.linspace(40000, 60000, 64),
            "real_gdp": np.linspace(38000, 55000, 64),
            "gdp_growth_yoy": np.random.uniform(-2, 5, 64),
            "gdp_growth_qoq": np.random.uniform(-2, 3, 64),
            "gdp_per_capita": np.linspace(15000, 25000, 64),
            "source_key": 1,
        })
    elif pillar == "unemployment":
        return pd.DataFrame({
            "date_key": [f"{y}-{m:02d}" for y in range(2010, 2026) for m in range(1, 13)],
            "unemployment_rate": np.random.uniform(5, 15, 192),
            "youth_unemployment_rate": np.random.uniform(15, 40, 192),
            "long_term_unemployment_rate": np.random.uniform(2, 10, 192),
            "labour_force_participation_rate": np.random.uniform(55, 75, 192),
            "source_key": 1,
        })
    elif pillar == "credit":
        return pd.DataFrame({
            "date_key": [f"{y}-{m:02d}" for y in range(2010, 2026) for m in range(1, 13)],
            "total_credit": np.random.uniform(200000, 300000, 192),
            "credit_nfc": np.random.uniform(80000, 120000, 192),
            "credit_households": np.random.uniform(80000, 120000, 192),
            "npl_ratio": np.random.uniform(1, 10, 192),
            "source_key": 1,
        })
    elif pillar == "interest_rates":
        return pd.DataFrame({
            "date_key": [f"{y}-{m:02d}" for y in range(2010, 2026) for m in range(1, 13)],
            "ecb_main_refinancing_rate": np.random.uniform(0, 4, 192),
            "euribor_3m": np.random.uniform(-0.5, 3, 192),
            "euribor_6m": np.random.uniform(-0.3, 3.5, 192),
            "euribor_12m": np.random.uniform(0, 4, 192),
            "portugal_10y_bond_yield": np.random.uniform(0, 6, 192),
            "source_key": 1,
        })
    elif pillar == "inflation":
        return pd.DataFrame({
            "date_key": [f"{y}-{m:02d}" for y in range(2010, 2026) for m in range(1, 13)],
            "hicp": np.random.uniform(-1, 8, 192),
            "cpi": np.random.uniform(-0.5, 7, 192),
            "core_inflation": np.random.uniform(0, 5, 192),
            "source_key": 1,
        })
    elif pillar == "public_debt":
        keys = [f"2010-Q{q}" for q in range(1, 5)] * 16
        return pd.DataFrame({
            "date_key": keys[:64],
            "total_debt": np.linspace(150000, 280000, 64),
            "debt_to_gdp_ratio": np.linspace(80, 130, 64),
            "budget_deficit": np.random.uniform(-8, 2, 64),
            "external_debt_share": np.random.uniform(40, 70, 64),
            "source_key": 1,
        })
    raise ValueError(f"Unknown pillar: {pillar}")


@pytest.fixture
def valid_data():
    """Create a full set of valid processed DataFrames."""
    pillars = ["gdp", "unemployment", "credit", "interest_rates", "inflation", "public_debt"]
    return {p: _make_pillar_df(p) for p in pillars}


class TestDQReport:
    def test_summary_counts(self):
        report = DQReport()
        report.checks.append(CheckResult("a", "pass", "ok"))
        report.checks.append(CheckResult("b", "warn", "warning"))
        report.checks.append(CheckResult("c", "fail", "bad"))
        assert report.passed == 1
        assert report.warnings == 1
        assert report.failures == 1
        assert report.has_critical_failure

    def test_to_dict(self):
        report = DQReport(run_id="test123")
        report.checks.append(CheckResult("x", "pass", "ok"))
        d = report.to_dict()
        assert d["run_id"] == "test123"
        assert d["summary"]["total"] == 1

    def test_save_creates_file(self, tmp_path):
        report = DQReport(run_id="save_test")
        report.checks.append(CheckResult("x", "pass", "ok"))
        path = report.save(directory=tmp_path)
        assert path.exists()
        assert "save_test" in path.name


class TestSchemaCheck:
    def test_all_valid_schemas_pass(self, valid_data):
        checker = DataQualityChecker(valid_data)
        checker.check_schema()
        fails = [c for c in checker.report.checks if c.status == "fail"]
        assert len(fails) == 0

    def test_missing_column_detected(self, valid_data):
        valid_data["gdp"] = valid_data["gdp"].drop(columns=["nominal_gdp"])
        checker = DataQualityChecker(valid_data)
        checker.check_schema()
        fails = [c for c in checker.report.checks if c.status == "fail" and c.pillar == "gdp"]
        assert len(fails) == 1


class TestNotNullCheck:
    def test_valid_data_passes(self, valid_data):
        checker = DataQualityChecker(valid_data)
        checker.check_not_null()
        fails = [c for c in checker.report.checks if c.status == "fail"]
        assert len(fails) == 0

    def test_all_null_column_fails(self, valid_data):
        valid_data["gdp"]["nominal_gdp"] = None
        checker = DataQualityChecker(valid_data)
        checker.check_not_null()
        fails = [c for c in checker.report.checks if c.status == "fail"]
        assert len(fails) == 1


class TestRangeCheck:
    def test_valid_ranges_pass(self, valid_data):
        checker = DataQualityChecker(valid_data)
        checker.check_ranges()
        fails = [c for c in checker.report.checks if c.status == "fail"]
        assert len(fails) == 0

    def test_out_of_range_detected(self, valid_data):
        valid_data["unemployment"].loc[
            valid_data["unemployment"].index[0], "unemployment_rate"
        ] = 99.0
        checker = DataQualityChecker(valid_data)
        checker.check_ranges()
        warns = [c for c in checker.report.checks
                 if c.status == "warn" and "unemployment_rate" in c.name]
        assert len(warns) >= 1


class TestCompletenessCheck:
    def test_full_data_passes(self, valid_data):
        checker = DataQualityChecker(valid_data)
        checker.check_completeness()
        warns = [c for c in checker.report.checks if c.status == "warn"]
        assert len(warns) == 0

    def test_missing_rows_warned(self, valid_data):
        valid_data["gdp"] = valid_data["gdp"].iloc[:50]  # should be 64
        checker = DataQualityChecker(valid_data)
        checker.check_completeness()
        warns = [c for c in checker.report.checks if c.status == "warn" and c.pillar == "gdp"]
        assert len(warns) == 1


class TestConsistencyCheck:
    def test_valid_consistency_passes(self, valid_data):
        # Ensure credit components are less than total
        valid_data["credit"]["credit_nfc"] = valid_data["credit"]["total_credit"] * 0.4
        valid_data["credit"]["credit_households"] = valid_data["credit"]["total_credit"] * 0.4
        checker = DataQualityChecker(valid_data)
        checker.check_consistency()
        fails = [c for c in checker.report.checks if c.status == "fail"]
        assert len(fails) == 0


class TestFreshnessCheck:
    def test_fresh_data_passes(self, valid_data):
        checker = DataQualityChecker(valid_data)
        checker.check_freshness()
        passes = [c for c in checker.report.checks if c.status == "pass"]
        assert len(passes) >= 1


class TestRunAll:
    def test_run_all_produces_checks(self, valid_data):
        checker = DataQualityChecker(valid_data, run_id="test_run")
        report = checker.run_all()
        assert len(report.checks) >= 15
        assert report.run_id == "test_run"
