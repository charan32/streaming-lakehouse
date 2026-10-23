import copy

import pytest

from lakehouse.contracts import known_event_types, validate
from lakehouse.generator import generate_events


def clean_events():
    return list(generate_events(300, seed=1, bad_rate=0, dup_rate=0))


def sample(event_type):
    return next(e for e in clean_events() if e["event_type"] == event_type)


def test_every_event_type_has_a_contract():
    assert known_event_types() == {"page_view", "add_to_cart", "order_placed", "payment"}


def test_clean_generated_events_all_pass():
    events = clean_events()
    failures = [(e, validate(e)) for e in events if validate(e)]
    assert not failures, failures[:3]
    assert {e["event_type"] for e in events} == known_event_types()


@pytest.mark.parametrize(
    "event_type, mutate, expected",
    [
        ("page_view", lambda e: e.pop("user_id"), "user_id: is required"),
        ("page_view", lambda e: e.update(event_ts="yesterday"), "event_ts:"),
        ("page_view", lambda e: e.update(event_ts="2026-13-40T99:00:00Z"), "event_ts:"),
        ("page_view", lambda e: e["payload"].update(page="admin"), "payload/page:"),
        ("add_to_cart", lambda e: e["payload"].update(unit_price=-3.5), "payload/unit_price:"),
        ("add_to_cart", lambda e: e["payload"].update(quantity=0), "payload/quantity:"),
        ("order_placed", lambda e: e["payload"].update(currency="usd$"), "payload/currency:"),
        ("order_placed", lambda e: e["payload"].update(items=[]), "payload/items:"),
        ("payment", lambda e: e["payload"].update(status="pending??"), "payload/status:"),
        ("payment", lambda e: e.update(surprise_field=1), "event:"),
    ],
)
def test_contract_violations_are_reported_by_field(event_type, mutate, expected):
    event = copy.deepcopy(sample(event_type))
    mutate(event)
    errors = validate(event)
    assert any(err.startswith(expected) for err in errors), errors


def test_unknown_event_type_is_rejected():
    event = copy.deepcopy(sample("page_view"))
    event["event_type"] = "checkout_started"
    assert validate(event) == ["event_type: no contract for 'checkout_started' (schema_version 1)"]


def test_non_object_is_rejected():
    assert validate(["not", "an", "object"]) == ["event is not a JSON object"]
