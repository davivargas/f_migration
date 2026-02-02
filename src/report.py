from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional
from dataclasses import asdict
from typing import Any, Dict, Optional

from .validator import Issue
from .anomalies import AnomalyResult


class RiskLevel(str, Enum):
    """Coarse migration decision based on detected issues/anomalies."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass(frozen=True)
class Summary:
    """Aggregated results of a migration evaluation run."""
    accounts_count: int
    transactions_count: int
    vendors_count: Optional[int]
    issues: List[Issue]
    anomaly: Optional[AnomalyResult]
    risk: RiskLevel


def _risk_from_counts(issue_count: int, anomaly_count: int) -> RiskLevel:
    """Map total findings to a coarse risk level (simple heuristic)."""
    total = issue_count + anomaly_count
    if total == 0:
        return RiskLevel.LOW
    if total <= 10:
        return RiskLevel.MEDIUM
    return RiskLevel.HIGH


def build_summary(accounts_count: int,
                  transactions_count: int,
                  vendors_count: Optional[int],
                  issues: List[Issue],
                  anomaly: Optional[AnomalyResult]) -> Summary:
    """
    Build the final evaluation summary, including the derived risk level.

    Note: "Top N" outliers is treated as a manual review signal (MEDIUM) even when
    there are no other detected issues.
    """
    anomaly_count = 0 if anomaly is None else anomaly.count
    risk = _risk_from_counts(sum(i.count for i in issues), anomaly_count)

    # Top-N outliers is an inspection mode; it should not produce LOW risk.
    if not issues and anomaly is not None and anomaly.message.startswith("Top "):
        risk = RiskLevel.MEDIUM

    return Summary(
        accounts_count=accounts_count,
        transactions_count=transactions_count,
        vendors_count=vendors_count,
        issues=issues,
        anomaly=anomaly,
        risk=risk
    )


def format_summary(summary: Summary) -> str:
    """
    Human-readable CLI report.

    Prints counts and up to 2 examples per issue/anomaly to keep output readable.
    """
    lines: List[str] = []
    lines.append("Migration Summary")
    lines.append("-----------------")
    lines.append(f"Accounts processed: {summary.accounts_count}")
    lines.append(f"Transactions processed: {summary.transactions_count}")
    if summary.vendors_count is not None:
        lines.append(f"Vendors processed: {summary.vendors_count}")
    lines.append("")
    lines.append("Issues detected:")

    if not summary.issues and summary.anomaly is None:
        lines.append("- none")
    else:
        for issue in summary.issues:
            lines.append(f"- {issue.message} ({issue.count})")
            # Print only a small sample for quick inspection.
            for ex in issue.examples[:2]:
                ex_str = ", ".join(f"{k}={v}" for k, v in ex.items())
                lines.append(f"    example: {ex_str}")

        if summary.anomaly is not None:
            lines.append(f"- {summary.anomaly.message} ({summary.anomaly.count})")
            for ex in summary.anomaly.examples[:2]:
                ex_str = ", ".join(f"{k}={v}" for k, v in ex.items())
                lines.append(f"    example: {ex_str}")

    lines.append("")
    lines.append(f"Migration risk level: {summary.risk.value}")
    return "\n".join(lines)


def to_json_dict(summary: Summary, cleaning_stats: dict | None = None) -> Dict[str, Any]:
    """Machine-readable report for automation (CI, dashboards, regression tests)."""
    issues = []
    for i in summary.issues:
        issues.append({
            "category": i.category,
            "message": i.message,
            "count": i.count,
            "examples": i.examples
        })

    anomaly: Optional[Dict[str, Any]] = None
    if summary.anomaly is not None:
        anomaly = {
            "message": summary.anomaly.message,
            "count": summary.anomaly.count,
            "examples": summary.anomaly.examples
        }

    return {
        "counts": {
            "accounts": summary.accounts_count,
            "transactions": summary.transactions_count,
            "vendors": summary.vendors_count
        },
        "cleaning": cleaning_stats,
        "issues": issues,
        "anomaly": anomaly,
        "risk": summary.risk.value
    }
