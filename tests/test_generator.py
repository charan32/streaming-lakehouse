import csv
from pathlib import Path

from lakehouse.catalog_data import PRODUCT_BY_ID, PRODUCTS
from lakehouse.contracts import validate
from lakehouse.generator import generate_events

SEED_FILE = Path(__file__).parents[1] / "dbt" / "seeds" / "products.csv"


def test_same_seed_gives_same_stream():
    assert list(generate_events(50, seed=3)) == list(generate_events(50, seed=3))


def test_orders_add_up_and_payments_match():
    events = list(generate_events(500, seed=5, bad_rate=0, dup_rate=0))
    orders = {e["payload"]["order_id"]: e for e in events if e["event_type"] == "order_placed"}
    payments = [e for e in events if e["event_type"] == "payment"]
    assert orders and payments
    for order in orders.values():
        items = order["payload"]["items"]
        assert round(sum(i["quantity"] * i["unit_price"] for i in items), 2) == order["payload"]["total"]
        assert all(i["product_id"] in PRODUCT_BY_ID for i in items)
    for p in payments:
        assert p["payload"]["amount"] == orders[p["payload"]["order_id"]]["payload"]["total"]


def test_imperfections_are_injected_at_roughly_the_requested_rate():
    events = list(generate_events(3000, seed=9, bad_rate=0.02, dup_rate=0.01))
    bad = sum(1 for e in events if validate(e))
    ids = [e.get("event_id") for e in events]
    duplicates = len(ids) - len(set(ids))
    assert 0.01 < bad / len(events) < 0.03
    assert 0.005 < duplicates / len(events) < 0.015


def test_dbt_seed_matches_product_catalog():
    with SEED_FILE.open() as f:
        rows = list(csv.DictReader(f))
    assert [(r["product_id"], r["product_name"], r["category"], float(r["list_price"])) for r in rows] == [
        (p.product_id, p.name, p.category, p.price) for p in PRODUCTS
    ], "run `make seed` to regenerate dbt/seeds/products.csv"
