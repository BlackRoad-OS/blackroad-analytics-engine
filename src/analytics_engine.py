#!/usr/bin/env python3
"""BlackRoad Analytics Engine — metrics aggregation, trend analysis, and alerting."""
from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

# ── Terminal colours ──────────────────────────────────────────────────────────
GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
BOLD   = "\033[1m"
NC     = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "analytics_engine.db"


# ── Data models ───────────────────────────────────────────────────────────────
@dataclass
class Metric:
    id: Optional[int]
    name: str
    value: float
    unit: str
    tags: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def display(self) -> str:
        ts = self.timestamp[:19].replace("T", " ")
        unit_str = f" {self.unit}" if self.unit else ""
        tags_str = f"  [{self.tags}]" if self.tags else ""
        return (
            f"  {CYAN}{self.name:<30}{NC}"
            f"{BOLD}{self.value:.4f}{unit_str:<8}{NC}"
            f"  {ts}{tags_str}"
        )


@dataclass
class AlertRule:
    id: Optional[int]
    metric_name: str
    threshold: float
    operator: str           # gt | lt | eq
    message: str
    active: bool = True
    triggered_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class TrendReport:
    metric_name: str
    window_hours: int
    count: int
    mean: float
    median: float
    stdev: float
    min_val: float
    max_val: float
    p95: float
    trend: str              # rising | falling | stable
    change_pct: float


@dataclass
class PipelineSummary:
    total_metrics: int
    unique_names: int
    active_alerts: int
    triggered_alerts: int
    oldest_record: str
    newest_record: str


# ── Database ──────────────────────────────────────────────────────────────────
def _get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE TABLE IF NOT EXISTS metrics ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name TEXT NOT NULL,"
        "  value REAL NOT NULL,"
        "  unit TEXT DEFAULT '',"
        "  tags TEXT DEFAULT '',"
        "  timestamp TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS alert_rules ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  metric_name TEXT NOT NULL,"
        "  threshold REAL NOT NULL,"
        "  operator TEXT NOT NULL,"
        "  message TEXT NOT NULL,"
        "  active INTEGER DEFAULT 1,"
        "  triggered_count INTEGER DEFAULT 0,"
        "  created_at TEXT NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name);"
        "CREATE INDEX IF NOT EXISTS idx_metrics_ts   ON metrics(timestamp);"
    )
    conn.commit()
    return conn


# ── Engine ────────────────────────────────────────────────────────────────────
class AnalyticsEngine:
    """Core analytics pipeline: ingestion, aggregation, trend detection, alerting."""

    def __init__(self) -> None:
        self._db = _get_db()

    # ── Ingestion ──────────────────────────────────────────────────────────
    def record(self, name: str, value: float, *,
               unit: str = "", tags: str = "") -> Metric:
        """Ingest a data point and evaluate active alert rules."""
        ts = datetime.utcnow().isoformat()
        cur = self._db.execute(
            "INSERT INTO metrics (name, value, unit, tags, timestamp) VALUES (?,?,?,?,?)",
            (name, value, unit, tags, ts),
        )
        self._db.commit()
        m = Metric(id=cur.lastrowid, name=name, value=value,
                   unit=unit, tags=tags, timestamp=ts)
        self._evaluate_alerts(m)
        return m

    def batch_record(self, rows: List[tuple]) -> int:
        """Bulk-ingest metrics. Each row: (name, value[, unit[, tags]])."""
        ts = datetime.utcnow().isoformat()
        self._db.executemany(
            "INSERT INTO metrics (name, value, unit, tags, timestamp) VALUES (?,?,?,?,?)",
            [(r[0], r[1],
              r[2] if len(r) > 2 else "",
              r[3] if len(r) > 3 else "",
              ts) for r in rows],
        )
        self._db.commit()
        return len(rows)

    # ── Trend analysis ─────────────────────────────────────────────────────
    def analyze(self, name: str, hours: int = 24) -> TrendReport:
        """Compute statistical trend report over a rolling time window."""
        since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        rows = self._db.execute(
            "SELECT value FROM metrics WHERE name=? AND timestamp>=? ORDER BY timestamp",
            (name, since),
        ).fetchall()
        if not rows:
            raise ValueError(f"No data for '{name}' in the last {hours}h")
        vals = [r["value"] for r in rows]
        sorted_vals = sorted(vals)
        mean   = statistics.mean(vals)
        median = statistics.median(vals)
        stdev  = statistics.stdev(vals) if len(vals) > 1 else 0.0
        idx95  = min(int(len(sorted_vals) * 0.95), len(sorted_vals) - 1)
        p95    = sorted_vals[idx95]
        mid    = max(len(vals) // 2, 1)
        first_avg  = statistics.mean(vals[:mid])
        last_avg   = statistics.mean(vals[mid:]) if len(vals) > mid else first_avg
        change_pct = ((last_avg - first_avg) / (abs(first_avg) + 1e-9)) * 100
        trend = ("rising" if change_pct > 5
                 else "falling" if change_pct < -5
                 else "stable")
        return TrendReport(
            metric_name=name, window_hours=hours, count=len(vals),
            mean=round(mean, 4), median=round(median, 4),
            stdev=round(stdev, 4), min_val=min(vals), max_val=max(vals),
            p95=round(p95, 4), trend=trend, change_pct=round(change_pct, 2),
        )

    # ── Alerting ───────────────────────────────────────────────────────────
    def add_alert(self, metric_name: str, threshold: float,
                  operator: str, message: str) -> AlertRule:
        ts = datetime.utcnow().isoformat()
        cur = self._db.execute(
            "INSERT INTO alert_rules"
            " (metric_name, threshold, operator, message, created_at)"
            " VALUES (?,?,?,?,?)",
            (metric_name, threshold, operator, message, ts),
        )
        self._db.commit()
        return AlertRule(id=cur.lastrowid, metric_name=metric_name,
                         threshold=threshold, operator=operator,
                         message=message, created_at=ts)

    def _evaluate_alerts(self, m: Metric) -> None:
        rules = self._db.execute(
            "SELECT * FROM alert_rules WHERE metric_name=? AND active=1", (m.name,)
        ).fetchall()
        for rule in rules:
            op, thr = rule["operator"], rule["threshold"]
            fired = (
                (op == "gt" and m.value > thr)
                or (op == "lt" and m.value < thr)
                or (op == "eq" and abs(m.value - thr) < 1e-9)
            )
            if fired:
                self._db.execute(
                    "UPDATE alert_rules"
                    " SET triggered_count=triggered_count+1 WHERE id=?",
                    (rule["id"],),
                )
                self._db.commit()
                print(f"{RED}\u26a8  ALERT [{rule['id']}]:{NC} {rule['message']}"
                      f"  ({m.name}={m.value} {op} {thr})")

    # ── Listing & summary ──────────────────────────────────────────────────
    def list_metrics(self, limit: int = 25) -> List[Metric]:
        rows = self._db.execute(
            "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [
            Metric(r["id"], r["name"], r["value"],
                   r["unit"], r["tags"], r["timestamp"])
            for r in rows
        ]

    def pipeline_summary(self) -> PipelineSummary:
        db = self._db
        total  = db.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        unique = db.execute(
            "SELECT COUNT(DISTINCT name) FROM metrics").fetchone()[0]
        active = db.execute(
            "SELECT COUNT(*) FROM alert_rules WHERE active=1").fetchone()[0]
        trig   = db.execute(
            "SELECT COUNT(*) FROM alert_rules WHERE triggered_count>0").fetchone()[0]
        oldest = db.execute(
            "SELECT MIN(timestamp) FROM metrics").fetchone()[0] or "\u2014"
        newest = db.execute(
            "SELECT MAX(timestamp) FROM metrics").fetchone()[0] or "\u2014"
        return PipelineSummary(
            total, unique, active, trig,
            oldest[:19].replace("T", " "),
            newest[:19].replace("T", " "),
        )

    def export(self, output: str = "analytics_export.json") -> Path:
        """Dump all metrics and alert rules to JSON."""
        metrics = self._db.execute(
            "SELECT * FROM metrics ORDER BY timestamp").fetchall()
        alerts = self._db.execute(
            "SELECT * FROM alert_rules").fetchall()
        data = {
            "exported_at": datetime.utcnow().isoformat(),
            "metrics":     [dict(r) for r in metrics],
            "alert_rules": [dict(r) for r in alerts],
        }
        out = Path(output)
        out.write_text(json.dumps(data, indent=2))
        return out


# ── Helpers ───────────────────────────────────────────────────────────────────
def _trend_icon(trend: str) -> str:
    return {
        "rising":  f"{GREEN}\u2191 rising{NC}",
        "falling": f"{RED}\u2193 falling{NC}",
        "stable":  f"{CYAN}\u2192 stable{NC}",
    }.get(trend, trend)


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="analytics_engine",
        description=f"{BOLD}BlackRoad Analytics Engine{NC} — metrics, trends, alerts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd", metavar="command")

    p_list = sub.add_parser("list", help="List recent metric data points")
    p_list.add_argument("--limit", type=int, default=25)

    p_add = sub.add_parser("add", help="Record a metric value")
    p_add.add_argument("name")
    p_add.add_argument("value", type=float)
    p_add.add_argument("--unit",  default="")
    p_add.add_argument("--tags",  default="")

    p_status = sub.add_parser("status", help="Trend analysis for a metric")
    p_status.add_argument("name")
    p_status.add_argument("--hours", type=int, default=24)

    p_alert = sub.add_parser("alert", help="Create an alert rule")
    p_alert.add_argument("metric")
    p_alert.add_argument("--threshold", type=float, required=True)
    p_alert.add_argument("--op", choices=["gt", "lt", "eq"],
                         default="gt", dest="operator")
    p_alert.add_argument("--message", default="Threshold breached")

    p_export = sub.add_parser("export", help="Export all data to JSON")
    p_export.add_argument("--output", default="analytics_export.json")

    args = parser.parse_args()
    engine = AnalyticsEngine()

    if args.cmd == "list":
        metrics = engine.list_metrics(args.limit)
        s = engine.pipeline_summary()
        print(f"\n{BOLD}{CYAN}\U0001f4ca Analytics Pipeline — "
              f"{s.total_metrics} points, {s.unique_names} metrics{NC}\n")
        for m in metrics:
            print(m.display())
        print(f"\n  alerts active={s.active_alerts}"
              f"  triggered={s.triggered_alerts}"
              f"  newest={s.newest_record}\n")

    elif args.cmd == "add":
        m = engine.record(args.name, args.value, unit=args.unit, tags=args.tags)
        print(f"{GREEN}\u2705 Recorded{NC}  {m.name} = {BOLD}{m.value}{NC}"
              f"{' ' + m.unit if m.unit else ''}  @{m.timestamp[:19]}")

    elif args.cmd == "status":
        try:
            r = engine.analyze(args.name, hours=args.hours)
        except ValueError as exc:
            print(f"{RED}\u2717 {exc}{NC}", file=sys.stderr)
            sys.exit(1)
        print(f"\n{BOLD}{CYAN}\U0001f4c8 Trend Report — {r.metric_name}{NC}")
        print(f"  Window   : last {r.window_hours}h  |  Samples: {r.count}")
        print(f"  Mean     : {r.mean}")
        print(f"  Median   : {r.median}")
        print(f"  Std Dev  : {r.stdev}")
        print(f"  p95      : {r.p95}")
        print(f"  Range    : {r.min_val}  \u2192  {r.max_val}")
        print(f"  Trend    : {_trend_icon(r.trend)}  ({r.change_pct:+.1f}%)\n")

    elif args.cmd == "alert":
        rule = engine.add_alert(
            args.metric, args.threshold, args.operator, args.message)
        print(f"{GREEN}\u2705 Alert created{NC}  id={rule.id}"
              f"  {rule.metric_name} {rule.operator} {rule.threshold}"
              f"  \u2192 \"{rule.message}\"")

    elif args.cmd == "export":
        out = engine.export(args.output)
        print(f"{GREEN}\u2705 Exported \u2192{NC} {out}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
