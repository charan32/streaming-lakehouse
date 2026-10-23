-- An order's total must equal the sum of its line items (to the cent).
select o.order_id, o.order_total, sum(i.line_total) as items_total
from {{ ref('stg_orders') }} as o
join {{ ref('stg_order_items') }} as i using (order_id)
group by o.order_id, o.order_total
having abs(o.order_total - sum(i.line_total)) > 0.01
