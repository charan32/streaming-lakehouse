-- A payment must be for the exact amount of the order it pays for.
select p.order_id, p.amount, o.order_total
from {{ ref('stg_payments') }} as p
join {{ ref('stg_orders') }} as o using (order_id)
where abs(p.amount - o.order_total) > 0.01
