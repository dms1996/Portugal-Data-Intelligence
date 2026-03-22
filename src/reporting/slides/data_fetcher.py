"""
DataFetcher — database queries for the presentation generator.
"""

import sqlite3

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataFetcher:
    """Fetches all required metrics from the project database."""

    def __init__(self, db_path):
        self.db_path = str(db_path)
        logger.info(f"Connecting to database: {self.db_path}")

    def _query(self, sql, params=None):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(sql, params or ())
            return cur.fetchall()

    def _scalar(self, sql, params=None):
        rows = self._query(sql, params)
        if rows:
            return rows[0][0]
        return None

    # --- GDP ----------------------------------------------------------------
    def gdp_latest(self):
        row = self._query(
            "SELECT nominal_gdp, real_gdp, gdp_growth_yoy, gdp_per_capita, date_key "
            "FROM fact_gdp ORDER BY date_key DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    def gdp_growth_avg_by_period(self):
        """Return average YoY GDP growth per macro-economic period."""
        periods = {
            "Pre-crisis (2010-2011)": ("2010-Q1", "2011-Q4"),
            "Troika (2012-2014)": ("2012-Q1", "2014-Q4"),
            "Recovery (2015-2019)": ("2015-Q1", "2019-Q4"),
            "COVID (2020-2021)": ("2020-Q1", "2021-Q4"),
            "Post-COVID (2022+)": ("2022-Q1", "2099-Q4"),
        }
        results = {}
        for label, (start, end) in periods.items():
            avg = self._scalar(
                "SELECT AVG(gdp_growth_yoy) FROM fact_gdp "
                "WHERE date_key BETWEEN ? AND ? AND gdp_growth_yoy IS NOT NULL",
                (start, end),
            )
            results[label] = avg
        return results

    def gdp_min_max(self):
        mn = self._query(
            "SELECT date_key, gdp_growth_yoy FROM fact_gdp "
            "WHERE gdp_growth_yoy IS NOT NULL ORDER BY gdp_growth_yoy ASC LIMIT 1"
        )
        mx = self._query(
            "SELECT date_key, gdp_growth_yoy FROM fact_gdp "
            "WHERE gdp_growth_yoy IS NOT NULL ORDER BY gdp_growth_yoy DESC LIMIT 1"
        )
        return {
            "min_quarter": dict(mn[0]) if mn else {},
            "max_quarter": dict(mx[0]) if mx else {},
        }

    # --- Unemployment -------------------------------------------------------
    def unemployment_latest(self):
        row = self._query(
            "SELECT unemployment_rate, youth_unemployment_rate, "
            "long_term_unemployment_rate, labour_force_participation_rate, date_key "
            "FROM fact_unemployment ORDER BY date_key DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    def unemployment_peak(self):
        row = self._query(
            "SELECT unemployment_rate, date_key FROM fact_unemployment "
            "ORDER BY unemployment_rate DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    # --- Credit -------------------------------------------------------------
    def credit_latest(self):
        row = self._query(
            "SELECT total_credit, credit_nfc, credit_households, npl_ratio, date_key "
            "FROM fact_credit ORDER BY date_key DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    def credit_npl_peak(self):
        row = self._query(
            "SELECT npl_ratio, date_key FROM fact_credit "
            "ORDER BY npl_ratio DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    # --- Interest Rates -----------------------------------------------------
    def interest_rates_latest(self):
        row = self._query(
            "SELECT ecb_main_refinancing_rate, euribor_3m, euribor_12m, "
            "portugal_10y_bond_yield, date_key "
            "FROM fact_interest_rates ORDER BY date_key DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    def sovereign_spread_latest(self):
        """PT 10Y minus Germany proxy (Euribor 12m as rough proxy)."""
        row = self._query(
            "SELECT portugal_10y_bond_yield - euribor_12m AS spread, date_key "
            "FROM fact_interest_rates ORDER BY date_key DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    def sovereign_yield_peak(self):
        row = self._query(
            "SELECT portugal_10y_bond_yield, date_key FROM fact_interest_rates "
            "ORDER BY portugal_10y_bond_yield DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    # --- Inflation ----------------------------------------------------------
    def inflation_latest(self):
        row = self._query(
            "SELECT hicp, cpi, core_inflation, date_key "
            "FROM fact_inflation ORDER BY date_key DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    def inflation_peak(self):
        row = self._query(
            "SELECT hicp, date_key FROM fact_inflation "
            "ORDER BY hicp DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    # --- Public Debt --------------------------------------------------------
    def public_debt_latest(self):
        row = self._query(
            "SELECT total_debt, debt_to_gdp_ratio, budget_deficit, "
            "external_debt_share, date_key "
            "FROM fact_public_debt ORDER BY date_key DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    def public_debt_peak(self):
        row = self._query(
            "SELECT debt_to_gdp_ratio, date_key FROM fact_public_debt "
            "ORDER BY debt_to_gdp_ratio DESC LIMIT 1"
        )
        return dict(row[0]) if row else {}

    # --- Executive Summary helpers ------------------------------------------
    def record_counts(self):
        counts = {}
        for table in ["fact_gdp", "fact_unemployment", "fact_credit",
                       "fact_interest_rates", "fact_inflation", "fact_public_debt"]:
            counts[table] = self._scalar(f"SELECT COUNT(*) FROM {table}")
        return counts

    def gdp_total_growth(self):
        """Nominal GDP first vs last to compute total growth."""
        first = self._scalar(
            "SELECT nominal_gdp FROM fact_gdp ORDER BY date_key ASC LIMIT 1"
        )
        last = self._scalar(
            "SELECT nominal_gdp FROM fact_gdp ORDER BY date_key DESC LIMIT 1"
        )
        if first and last and first > 0:
            return ((last - first) / first) * 100
        return None
