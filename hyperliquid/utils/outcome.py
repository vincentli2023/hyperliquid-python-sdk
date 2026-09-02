"""HIP-4 outcome market helpers. Pure functions, no network.

Encoding (docs: for-developers/api/asset-ids):

    encoding = 10 * outcome + side         side is 0 (first sideSpec, usually Yes) or 1 (No)
    spot coin name   = "#<encoding>"       e.g. "#13380"  -> outcome 1338, side 0
    balance token    = "+<encoding>"       as it appears in spotClearinghouseState.balances
    order asset id   = 100_000_000 + encoding   the "a" field of an order action

Verified against a live mainnet order action: coin "#13390" was submitted with a=100013390.

Metadata comes from two info requests:

    {"type": "outcomeMeta"}       -> outcomes[]: {outcome, name: "template:<id>", description: "k:v|k:v", sideSpecs, ...}
    {"type": "outcomeTemplates"}  -> [{id, name: "{perp} above {threshold} at {time}?", description, keywords, role}]

Settled outcomes disappear from outcomeMeta, so nothing here requires metadata: the asset mapping is
arithmetic, and label rendering falls back to the raw coin name when metadata is missing.
"""

import datetime
from typing import Any, Dict, Optional, Tuple

OUTCOME_ASSET_BASE = 100_000_000
OUTCOME_TIME_FORMAT = "%Y%m%d-%H%M"  # template keyword type "dateTime", UTC
# description keys that carry the settlement / decision time, in priority order
OUTCOME_TIME_KEYS = ("time", "expiry", "scheduledDecision", "decisionDeadline")


def is_outcome_coin(name: str) -> bool:
    return isinstance(name, str) and len(name) > 1 and name[0] == "#" and name[1:].isdigit()


def outcome_encoding(outcome: int, side: int) -> int:
    if side not in (0, 1):
        raise ValueError(f"outcome side must be 0 or 1, got {side}")
    if outcome < 0:
        raise ValueError(f"outcome id must be non-negative, got {outcome}")
    return 10 * outcome + side


def parse_outcome_coin(name: str) -> Tuple[int, int]:
    """'#13381' -> (1338, 1)"""
    if not is_outcome_coin(name):
        raise ValueError(f"not an outcome coin: {name!r}")
    encoding = int(name[1:])
    return encoding // 10, encoding % 10


def outcome_coin(outcome: int, side: int) -> str:
    return f"#{outcome_encoding(outcome, side)}"


def outcome_token(outcome: int, side: int) -> str:
    return f"+{outcome_encoding(outcome, side)}"


def outcome_asset(name: str) -> int:
    """Asset id for the order action's "a" field."""
    outcome, side = parse_outcome_coin(name)
    return OUTCOME_ASSET_BASE + outcome_encoding(outcome, side)


def parse_outcome_description(description: str) -> Dict[str, str]:
    """'perp:xyz:SILVER|seconds:3|time:20260902-2100' -> {'perp': 'xyz:SILVER', 'seconds': '3', 'time': '20260902-2100'}

    Values keep everything after the first ':' so perp names like 'xyz:SILVER' survive.
    Segments without ':' (e.g. the question fallback description 'other') map to ''.
    """
    out: Dict[str, str] = {}
    for segment in description.split("|"):
        if not segment:
            continue
        key, sep, value = segment.partition(":")
        out[key] = value if sep else ""
    return out


def parse_outcome_time_ms(value: str) -> int:
    """'20260902-2100' (UTC) -> epoch milliseconds."""
    dt = datetime.datetime.strptime(value, OUTCOME_TIME_FORMAT).replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def format_outcome_time(value: str) -> str:
    """'20260902-2100' -> '2026-09-02 21:00 UTC'; unparseable values are returned unchanged."""
    try:
        dt = datetime.datetime.strptime(value, OUTCOME_TIME_FORMAT)
    except ValueError:
        return value
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def settle_time_ms(description: str) -> Optional[int]:
    """Settlement / decision time from a description, or None when no time-like key is present."""
    fields = parse_outcome_description(description)
    for key in OUTCOME_TIME_KEYS:
        value = fields.get(key)
        if value:
            try:
                return parse_outcome_time_ms(value)
            except ValueError:
                continue
    return None


def template_id(meta_name: str) -> Optional[str]:
    """'template:binaryPrice' -> 'binaryPrice'; non-template names -> None."""
    prefix = "template:"
    if meta_name.startswith(prefix):
        return meta_name[len(prefix) :]
    return None


def render_template(text: str, values: Dict[str, str]) -> str:
    """Substitute {keyword} placeholders; unknown keywords are left as-is."""
    for key, value in values.items():
        text = text.replace("{" + key + "}", value)
    return text


def side_name(meta_entry: Dict[str, Any], side: int) -> str:
    try:
        raw = meta_entry["sideSpecs"][side]["name"]
    except (KeyError, IndexError, TypeError):
        return "Yes" if side == 0 else "No"
    return raw[len("template:") :] if raw.startswith("template:") else raw


def outcome_label(
    name: str,
    meta_entry: Optional[Dict[str, Any]] = None,
    templates_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """Human-readable label for an outcome coin.

    With template + meta: 'xyz:SILVER above 64.128 at 2026-09-02 21:00 UTC? Yes'  (matches outcome.xyz's title)
    With meta only:       'binaryPrice perp:xyz:SILVER|...|time:20260902-2100 Yes'
    Without meta:         '#13460'
    """
    if not is_outcome_coin(name):
        return name
    if not meta_entry:
        return name
    _, side = parse_outcome_coin(name)
    side_str = side_name(meta_entry, side)
    fields = parse_outcome_description(meta_entry.get("description", ""))
    tid = template_id(meta_entry.get("name", ""))
    template = (templates_by_id or {}).get(tid) if tid else None
    if template and template.get("name"):
        display = {k: (format_outcome_time(v) if k in OUTCOME_TIME_KEYS else v) for k, v in fields.items()}
        return f"{render_template(template['name'], display)} {side_str}"
    head = tid or meta_entry.get("name", "")
    return f"{head} {meta_entry.get('description', '')} {side_str}".strip()
