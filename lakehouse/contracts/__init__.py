"""Data contracts: one versioned JSON Schema per event type.

A contract is the agreement between the teams that produce events and the
lakehouse that stores them. Events that break it never reach the bronze table;
they go to a dead-letter topic and the ``bronze.events_rejected`` table with a
readable reason, so the producing team can fix them.
"""

from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACT_DIR = Path(__file__).parent


@lru_cache(maxsize=None)
def validators() -> dict[tuple[str, int], Draft202012Validator]:
    out = {}
    for path in sorted(CONTRACT_DIR.glob("*.v*.json")):
        name, version = path.name.removesuffix(".json").split(".v")
        schema = json.loads(path.read_text())
        Draft202012Validator.check_schema(schema)
        out[(name, int(version))] = Draft202012Validator(schema)
    return out


def known_event_types() -> set[str]:
    return {name for name, _ in validators()}


def validate(event: object) -> list[str]:
    """Return a list of human-readable contract violations (empty when valid)."""
    if not isinstance(event, dict):
        return ["event is not a JSON object"]
    event_type = event.get("event_type")
    version = event.get("schema_version", 1)
    validator = validators().get((event_type, version)) if isinstance(version, int) else None
    if validator is None:
        return [f"event_type: no contract for {event_type!r} (schema_version {version!r})"]

    errors = []
    for err in sorted(validator.iter_errors(event), key=lambda e: [str(p) for p in e.absolute_path]):
        path = [str(p) for p in err.absolute_path]
        if err.validator == "required":
            # "'user_id' is a required property" -> "user_id: is required"
            missing = err.message.split("'")[1] if "'" in err.message else "?"
            errors.append(f"{'/'.join(path + [missing])}: is required")
            continue
        errors.append(f"{'/'.join(path) or 'event'}: {err.message}")

    ts = event.get("event_ts")
    if isinstance(ts, str) and not any(e.startswith("event_ts") for e in errors):
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"event_ts: {ts!r} is not a real date and time")
    return errors
