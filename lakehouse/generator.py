"""Synthetic e-commerce clickstream.

Each simulated session browses a few pages, may add items to a cart, may place
an order, and an order is followed by a payment that is usually authorized.

To make the pipeline earn its keep, the stream is deliberately imperfect:

* ``bad_rate``  – events that break the data contract (missing fields, negative
                  prices, unknown event types, bad currency, unparseable time).
* ``dup_rate``  – exact replays of an event, as a producer retry would cause.
* ``late_rate`` – events stamped minutes in the past, as a buffering mobile
                  client would send them.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

from .catalog_data import PRODUCTS

PAGES = ["home", "category", "product", "cart", "checkout"]
PAYMENT_METHODS = ["card", "paypal", "apple_pay"]


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class EventFactory:
    def __init__(self, rng: random.Random):
        self.rng = rng

    def uid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def event(self, event_type: str, ts: datetime, user_id: str, session_id: str, payload: dict) -> dict:
        return {
            "event_id": self.uid(),
            "event_type": event_type,
            "event_ts": _iso(ts),
            "user_id": user_id,
            "session_id": session_id,
            "schema_version": 1,
            "payload": payload,
        }


def _session(f: EventFactory, start: datetime) -> list[dict]:
    rng = f.rng
    user_id = f"U{rng.randint(1, 800):04d}"
    session_id = f.uid()
    t = start
    out: list[dict] = []

    def tick() -> datetime:
        nonlocal t
        t += timedelta(seconds=rng.uniform(2, 40))
        return t

    for _ in range(rng.randint(1, 6)):
        page = rng.choice(PAGES[:3])
        payload = {"page": page}
        if page == "product":
            payload["product_id"] = rng.choice(PRODUCTS).product_id
        out.append(f.event("page_view", tick(), user_id, session_id, payload))

    cart: list[dict] = []
    if rng.random() < 0.38:
        for p in rng.sample(PRODUCTS, rng.randint(1, 3)):
            qty = rng.choices([1, 2, 3], weights=[7, 2, 1])[0]
            cart.append({"product_id": p.product_id, "quantity": qty, "unit_price": p.price})
            out.append(f.event("add_to_cart", tick(), user_id, session_id,
                               {"product_id": p.product_id, "quantity": qty, "unit_price": p.price}))

    if cart and rng.random() < 0.55:
        out.append(f.event("page_view", tick(), user_id, session_id, {"page": "checkout"}))
        order_id = f"O-{f.uid()[:13]}"
        total = round(sum(i["quantity"] * i["unit_price"] for i in cart), 2)
        out.append(f.event("order_placed", tick(), user_id, session_id,
                           {"order_id": order_id, "items": cart, "total": total, "currency": "USD"}))
        status = "authorized" if rng.random() < 0.93 else "declined"
        out.append(f.event("payment", tick(), user_id, session_id,
                           {"order_id": order_id, "amount": total,
                            "method": rng.choice(PAYMENT_METHODS), "status": status}))
    return out


def _corrupt(rng: random.Random, e: dict) -> dict:
    """Return a copy of ``e`` that violates the data contract in one realistic way."""
    bad = {**e, "payload": dict(e["payload"])}
    options = ["drop_user", "unknown_type", "bad_timestamp"]
    if e["event_type"] == "add_to_cart":
        options.append("negative_price")
    if e["event_type"] == "order_placed":
        options += ["bad_currency", "empty_items"]
    if e["event_type"] == "payment":
        options.append("bad_status")
    kind = rng.choice(options)
    if kind == "drop_user":
        bad.pop("user_id")
    elif kind == "unknown_type":
        bad["event_type"] = "checkout_started"
    elif kind == "bad_timestamp":
        bad["event_ts"] = "yesterday"
    elif kind == "negative_price":
        bad["payload"]["unit_price"] = -abs(bad["payload"]["unit_price"])
    elif kind == "bad_currency":
        bad["payload"]["currency"] = "usd$"
    elif kind == "empty_items":
        bad["payload"]["items"] = []
    elif kind == "bad_status":
        bad["payload"]["status"] = "pending??"
    return bad


def generate_events(
    n_sessions: int,
    seed: int = 7,
    start: datetime | None = None,
    bad_rate: float = 0.02,
    dup_rate: float = 0.01,
    late_rate: float = 0.03,
    now_fn: Callable[[], datetime] | None = None,
) -> Iterator[dict]:
    """Yield events for ``n_sessions`` sessions.

    With ``now_fn`` set (live streaming), events are stamped with the current
    wall-clock time instead of simulated time.
    """
    rng = random.Random(seed)
    f = EventFactory(rng)
    clock = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    for _ in range(n_sessions):
        clock += timedelta(seconds=rng.expovariate(1 / 20))
        for e in _session(f, clock):
            if now_fn is not None:
                e["event_ts"] = _iso(now_fn())
            if rng.random() < late_rate:
                ts = datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00"))
                e["event_ts"] = _iso(ts - timedelta(minutes=rng.randint(5, 30)))
            if rng.random() < bad_rate:
                e = _corrupt(rng, e)
            yield e
            if rng.random() < dup_rate:
                yield e
