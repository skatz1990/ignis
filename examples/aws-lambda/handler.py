"""
AWS Lambda handler: analyze a Spark event log with ignis when it lands in S3.

Triggered by an S3 ObjectCreated event. Runs ignis against the new object,
then posts findings to Slack if any are found.

Environment variables
---------------------
IGNIS_SLACK_WEBHOOK   Slack incoming webhook URL (required for notifications)
IGNIS_MIN_SEVERITY    Minimum severity to notify on: WARNING or INFO (default: WARNING)
"""

import json
import logging
import os

from ignis.notify.slack import post as slack_post
from ignis.parser.event_log import parse_event_log
from ignis.rules.base import Severity
from ignis.rules.failed_tasks import FailedTasksRule
from ignis.rules.gc_pressure import GCPressureRule
from ignis.rules.partition import PartitionCountRule
from ignis.rules.shuffle import ShuffleSizeRule
from ignis.rules.skew import DataSkewRule
from ignis.rules.spill import SpillRule

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_RULES = [
    DataSkewRule(),
    ShuffleSizeRule(),
    SpillRule(),
    PartitionCountRule(),
    FailedTasksRule(),
    GCPressureRule(),
]


def handler(event: dict, context: object) -> dict:
    """Entry point for the Lambda function."""
    webhook_url = os.environ.get("IGNIS_SLACK_WEBHOOK")
    min_severity = os.environ.get("IGNIS_MIN_SEVERITY", "WARNING").upper()

    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        path = f"s3://{bucket}/{key}"

        logger.info("Analyzing %s", path)

        try:
            app = parse_event_log(path)
        except Exception:
            logger.exception("Failed to parse %s", path)
            results.append({"path": path, "status": "parse_error"})
            continue

        findings = [f for rule in _RULES for f in rule.analyze(app)]

        if min_severity == "WARNING":
            findings = [f for f in findings if f.severity == Severity.WARNING]

        logger.info("%d finding(s) for %s", len(findings), path)

        if findings and webhook_url:
            payload = {
                "app_id": app.app_id,
                "app_name": app.app_name,
                "finding_count": len(findings),
                "findings": [
                    {
                        "rule": f.rule,
                        "severity": f.severity.value,
                        "stage_id": f.stage_id,
                        "stage_name": f.stage_name,
                        "message": f.message,
                        "recommendation": f.recommendation,
                    }
                    for f in findings
                ],
            }
            try:
                slack_post(webhook_url, payload)
                logger.info("Slack notification sent for %s", path)
            except Exception:
                logger.exception("Slack notification failed for %s", path)

        results.append(
            {
                "path": path,
                "app_id": app.app_id,
                "app_name": app.app_name,
                "finding_count": len(findings),
                "status": "ok",
            }
        )

    return {"statusCode": 200, "body": json.dumps(results)}
