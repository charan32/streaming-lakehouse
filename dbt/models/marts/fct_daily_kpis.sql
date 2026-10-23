-- The numbers a business dashboard would show, one row per day.
with sessions as (
    select
        session_date as day,
        count(*)                                 as sessions,
        count(*) filter (where added_to_cart)    as cart_sessions,
        count(*) filter (where ordered)          as order_sessions,
        count(*) filter (where paid)             as paid_sessions
    from {{ ref('fct_sessions') }}
    group by 1
),

orders as (
    select
        order_date as day,
        count(*)                                     as orders,
        count(*) filter (where is_paid)              as paid_orders,
        sum(order_total) filter (where is_paid)      as revenue
    from {{ ref('fct_orders') }}
    group by 1
)

select
    s.day,
    s.sessions,
    coalesce(o.orders, 0)                                         as orders,
    coalesce(o.paid_orders, 0)                                    as paid_orders,
    coalesce(o.revenue, 0)                                        as revenue,
    round(coalesce(o.revenue, 0) / nullif(o.paid_orders, 0), 2)   as avg_order_value,
    round(s.cart_sessions  / s.sessions, 4)                       as cart_rate,
    round(s.order_sessions / s.sessions, 4)                       as order_rate,
    round(s.paid_sessions  / s.sessions, 4)                       as conversion_rate
from sessions as s
left join orders as o using (day)
order by s.day
