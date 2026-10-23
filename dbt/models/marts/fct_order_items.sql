select
    i.order_id,
    o.order_date,
    o.user_id,
    o.is_paid,
    i.product_id,
    p.category,
    i.quantity,
    i.unit_price,
    i.line_total
from {{ ref('stg_order_items') }} as i
join {{ ref('fct_orders') }} as o using (order_id)
left join {{ ref('dim_products') }} as p using (product_id)
