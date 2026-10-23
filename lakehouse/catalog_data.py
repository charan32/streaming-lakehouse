"""The product catalog used by the event generator.

It is deterministic, so the same product IDs and prices appear in every run.
``dbt/seeds/products.csv`` is generated from this list (``python -m
lakehouse.catalog_data``) and a test checks the two never drift apart.
"""

from __future__ import annotations

import csv
import random
import sys
from dataclasses import dataclass

CATEGORIES = {
    "electronics": (25.0, 450.0),
    "home": (8.0, 180.0),
    "apparel": (12.0, 120.0),
    "beauty": (6.0, 75.0),
    "sports": (10.0, 220.0),
}

ADJECTIVES = ["Classic", "Smart", "Eco", "Pro", "Compact", "Everyday", "Premium", "Travel"]
NOUNS = {
    "electronics": ["Headphones", "Speaker", "Charger", "Keyboard", "Monitor Arm", "Webcam", "Earbuds", "Power Bank"],
    "home": ["Mug Set", "Lamp", "Throw Blanket", "Cutting Board", "Plant Pot", "Candle", "Organizer", "Kettle"],
    "apparel": ["Hoodie", "Tee", "Rain Jacket", "Socks Pack", "Cap", "Joggers", "Polo", "Scarf"],
    "beauty": ["Face Wash", "Serum", "Lip Balm", "Sunscreen", "Shampoo", "Hand Cream", "Toner", "Body Lotion"],
    "sports": ["Yoga Mat", "Water Bottle", "Resistance Bands", "Running Belt", "Gym Bag", "Jump Rope", "Foam Roller", "Dumbbell Set"],
}


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    price: float


def build_products(seed: int = 42) -> list[Product]:
    rng = random.Random(seed)
    products: list[Product] = []
    n = 0
    for category, (low, high) in CATEGORIES.items():
        for noun in NOUNS[category]:
            n += 1
            name = f"{rng.choice(ADJECTIVES)} {noun}"
            price = round(rng.uniform(low, high), 2)
            products.append(Product(f"P{n:03d}", name, category, price))
    return products


PRODUCTS = build_products()
PRODUCT_BY_ID = {p.product_id: p for p in PRODUCTS}


def write_seed(path: str) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "list_price"])
        for p in PRODUCTS:
            w.writerow([p.product_id, p.name, p.category, f"{p.price:.2f}"])


if __name__ == "__main__":
    write_seed(sys.argv[1] if len(sys.argv) > 1 else "dbt/seeds/products.csv")
