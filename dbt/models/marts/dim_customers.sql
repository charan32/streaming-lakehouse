with activity as (
    select
        user_id,
        min(event_ts)              as first_seen_at,
        max(event_ts)              as last_seen_at,
        count(distinct session_id) as sessions
    from {{ ref('stg_events') }}
    group by user_id
),

orders as (
    select
        user_id,
        count(*)                                        as orders,
        count(*) filter (where is_paid)                 as paid_orders,
        coalesce(sum(order_total) filter (where is_paid), 0) as lifetime_revenue
    from {{ ref('fct_orders') }}
    group by user_id
)

select
    a.user_id,
    a.first_seen_at,
    a.last_seen_at,
    a.sessions,
    coalesce(o.orders, 0)           as orders,
    coalesce(o.paid_orders, 0)      as paid_orders,
    coalesce(o.lifetime_revenue, 0) as lifetime_revenue,
    case
        when coalesce(o.paid_orders, 0) >= 3 then 'loyal'
        when coalesce(o.paid_orders, 0) >= 1 then 'customer'
        else 'browser'
    end                             as segment
from activity as a
left join orders as o using (user_id)
