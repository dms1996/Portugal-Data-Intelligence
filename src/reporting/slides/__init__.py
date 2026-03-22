"""
Slides package — modular slide builders for the executive presentation.
"""

from src.reporting.slides.data_fetcher import DataFetcher

from src.reporting.slides.helpers import (
    _set_slide_bg,
    _add_textbox,
    _add_rich_textbox,
    _add_paragraph,
    _add_kpi_box,
    _add_chart_image,
    _add_section_header,
    _add_footer,
    _fmt_number,
    _fmt_pct,
    _fmt_eur,
)

from src.reporting.slides.opening import (
    slide_title,
    slide_agenda,
    slide_executive_summary,
    _add_so_what_box,
    slide_scorecard,
)

from src.reporting.slides.economic_pillars import (
    slide_economic_dashboard,
    slide_gdp_analysis,
    slide_gdp_deep_dive,
    slide_labour_market,
    slide_interest_rates,
    slide_credit,
    slide_inflation,
    slide_public_debt,
)

from src.reporting.slides.cross_pillar import (
    slide_correlation,
    slide_phillips_curve,
    slide_crisis_timeline,
    slide_eu_benchmarking,
    slide_strategic_recommendations,
    slide_risk_matrix,
    slide_thank_you,
)

__all__ = [
    "DataFetcher",
    # Helpers
    "_set_slide_bg",
    "_add_textbox",
    "_add_rich_textbox",
    "_add_paragraph",
    "_add_kpi_box",
    "_add_chart_image",
    "_add_section_header",
    "_add_footer",
    "_fmt_number",
    "_fmt_pct",
    "_fmt_eur",
    # Opening
    "slide_title",
    "slide_agenda",
    "slide_executive_summary",
    "_add_so_what_box",
    "slide_scorecard",
    # Economic pillars
    "slide_economic_dashboard",
    "slide_gdp_analysis",
    "slide_gdp_deep_dive",
    "slide_labour_market",
    "slide_interest_rates",
    "slide_credit",
    "slide_inflation",
    "slide_public_debt",
    # Cross-pillar & strategic
    "slide_correlation",
    "slide_phillips_curve",
    "slide_crisis_timeline",
    "slide_eu_benchmarking",
    "slide_strategic_recommendations",
    "slide_risk_matrix",
    "slide_thank_you",
]
