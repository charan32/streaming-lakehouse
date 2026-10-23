-- One row per product in an order (the order event carries the items as a JSON array).
with orders as (
    select
        json_extract_string(payload, '$.order_id') as order_id,
        unnest(
            from_json(
                json_extract(payload, '$.items'),
                '[{"product_id": "VARCHAR", "quantity": "INTEGER", "unit_price": "DOUBLE"}]'
            )
        ) as item
    from {{ ref('stg_events') }}
    where event_type = 'order_placed'
)

select
    order_id,
    item.product_id                                         as product_id,
    item.quantity                                           as quantity,
    cast(item.unit_price as decimal(10, 2))                 as unit_price,
    cast(item.quantity * item.unit_price as decimal(12, 2)) as line_total
from orders
