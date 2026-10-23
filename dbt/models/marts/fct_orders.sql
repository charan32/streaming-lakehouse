-- One row per order with the outcome of its most recent payment attempt.
with latest_payment as (
    select *
    from {{ ref('stg_payments') }}
    qualify row_number() over (partition by order_id order by paid_at desc) = 1
)

select
    o.order_id,
    o.user_id,
    o.session_id,
    o.ordered_at,
    o.order_date,
    o.order_total,
    o.currency,
    o.line_count,
    p.payment_method,
    coalesce(p.payment_status, 'missing')       as payment_status,
    coalesce(p.payment_status = 'authorized', false) as is_paid,
    date_diff('second', o.ordered_at, p.paid_at) as seconds_to_payment
from {{ ref('stg_orders') }} as o
left join latest_payment as p using (order_id)
