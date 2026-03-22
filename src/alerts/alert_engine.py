"""
Portugal Data Intelligence — Alert Engine
============================================
Monitors macroeconomic indicators against configurable thresholds
and generates alerts when values breach warning or critical levels.

Usage:
    from src.alerts.alert_engine import AlertEngine
    engine = AlertEngine()
    alerts = engine.check_all()
"""

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from config.settings import CONFIG_DIR, DATABASE_PATH, REPORTS_DIR
from src.utils.logger import get_logger

logger = get_logger(__name__)

THRESHOLDS_FILE = CONFIG_DIR / "alert_thresholds.json"
ALERTS_DIR = REPORTS_DIR / "alerts"


@dataclass
class Alert:
    """A single threshold breach alert."""
    indicator: str
    description: str
    severity: str  # warning | critical
    value: float
    threshold: float
    direction: str  # above | below
    period: str
    timestamp: str


class AlertEngine:
    """Check latest indicator values against configurable thresholds.

    Parameters
    ----------
    db_path : Path, optional
        Override the default database path.
    thresholds_path : Path, optional
        Override the default thresholds JSON file.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        thresholds_path: Optional[Path] = None,
    ):
        self.db_path = db_path or DATABASE_PATH
        self.thresholds_path = thresholds_path or THRESHOLDS_FILE
        self.thresholds = self._load_thresholds()

    def _load_thresholds(self) -> Dict[str, Any]:
        """Load threshold definitions from JSON."""
        with open(self.thresholds_path, "r", encoding="utf-8") as f:
            return json.load(f)  # type: ignore[no-any-return]

    def _get_latest_value(
        self, conn: sqlite3.Connection, table: str, column: str
    ) -> Optional[tuple]:  # type: ignore[type-arg]
        """Return (date_key, value) for the most recent non-null observation."""
        try:
            row = conn.execute(
                f"SELECT date_key, {column} FROM {table} "
                f"WHERE {column} IS NOT NULL ORDER BY date_key DESC LIMIT 1"
            ).fetchone()
            return row  # type: ignore[return-value,no-any-return]
        except Exception as exc:
            logger.warning("Could not query %s.%s: %s", table, column, exc)
            return None

    def _check_indicator(
        self,
        indicator_key: str,
        config: Dict[str, Any],
        conn: sqlite3.Connection,
    ) -> List[Alert]:
        """Check a single indicator against its thresholds."""
        result = self._get_latest_value(conn, config["table"], config["column"])
        if result is None:
            return []

        date_key, value = result
        if value is None:
            return []

        alerts = []
        now = datetime.now(timezone.utc).isoformat()

        for severity in ("critical", "warning"):
            rules = config.get(severity, {})
            if "above" in rules and value > rules["above"]:
                alerts.append(Alert(
                    indicator=indicator_key,
                    description=config["description"],
                    severity=severity,
                    value=round(float(value), 2),
                    threshold=rules["above"],
                    direction="above",
                    period=str(date_key),
                    timestamp=now,
                ))
            if "below" in rules and value < rules["below"]:
                alerts.append(Alert(
                    indicator=indicator_key,
                    description=config["description"],
                    severity=severity,
                    value=round(float(value), 2),
                    threshold=rules["below"],
                    direction="below",
                    period=str(date_key),
                    timestamp=now,
                ))

        return alerts

    def check_all(self) -> List[Alert]:
        """Check all configured indicators and return any alerts.

        Returns
        -------
        list of Alert
            All triggered alerts, sorted by severity (critical first).
        """
        conn = sqlite3.connect(str(self.db_path))
        all_alerts: List[Alert] = []

        for key, config in self.thresholds.items():
            alerts = self._check_indicator(key, config, conn)
            for alert in alerts:
                log_fn = logger.critical if alert.severity == "critical" else logger.warning
                log_fn(
                    "ALERT [%s] %s: %s = %.2f (threshold: %s %.2f)",
                    alert.severity.upper(), alert.indicator,
                    alert.description, alert.value,
                    alert.direction, alert.threshold,
                )
            all_alerts.extend(alerts)

        conn.close()

        # Sort: critical first, then warning
        severity_order = {"critical": 0, "warning": 1}
        all_alerts.sort(key=lambda a: severity_order.get(a.severity, 2))

        logger.info("Alert check complete: %d alert(s) triggered", len(all_alerts))
        return all_alerts

    def save_alerts(self, alerts: List[Alert], directory: Optional[Path] = None) -> Path:
        """Save alerts to a timestamped JSON file."""
        out_dir = directory or ALERTS_DIR
        out_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = out_dir / f"alerts_{ts}.json"

        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_alerts": len(alerts),
            "critical": sum(1 for a in alerts if a.severity == "critical"),
            "warning": sum(1 for a in alerts if a.severity == "warning"),
            "alerts": [asdict(a) for a in alerts],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info("Alerts saved to %s", path)
        return path
