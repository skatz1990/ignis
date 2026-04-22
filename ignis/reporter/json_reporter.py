import json
import sys

from ignis.rules.base import Finding


def render_findings(findings: list[Finding], app_id: str, app_name: str) -> None:
    output = {
        "app_id": app_id,
        "app_name": app_name,
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
    sys.stdout.write(json.dumps(output, indent=2) + "\n")
