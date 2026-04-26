import json
import urllib.error
import urllib.request

_SEVERITY_EMOJI = {"WARNING": "⚠️", "INFO": "ℹ️", "ERROR": "🔴"}


def _build_payload(data: dict) -> dict:
    app_name = data.get("app_name", data.get("app_id", "unknown"))
    finding_count = data.get("finding_count", 0)
    findings = data.get("findings", [])

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"⚠️ ignis: {finding_count} issue(s) found — {app_name}",
            },
        }
    ]

    for f in findings:
        severity = f.get("severity", "").upper()
        emoji = _SEVERITY_EMOJI.get(severity, "•")
        rule = f.get("rule", "")
        stage = f.get("stage_name", f"Stage {f.get('stage_id', '?')}")
        message = f.get("message", "")
        recommendation = f.get("recommendation", "")

        text = f"{emoji} *{severity}* · `{rule}` · {stage}\n{message}"
        if recommendation:
            text += f"\n_{recommendation}_"

        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})

    # Remove trailing divider
    if blocks and blocks[-1]["type"] == "divider":
        blocks.pop()

    return {"blocks": blocks}


def post(webhook_url: str, data: dict) -> None:
    payload = _build_payload(data)
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Slack returned HTTP {resp.status}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Slack returned HTTP {exc.code}: {exc.read().decode()}") from exc
