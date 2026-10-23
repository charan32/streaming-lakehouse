-- One row per session with how far it got down the purchase funnel.
with events as (
    select * from {{ ref('stg_events') }}
),

orders as (
    select session_id, bool_or(is_paid) as paid
    from {{ ref('fct_orders') }}
    group by session_id
)

select
    e.session_id,
    any_value(e.user_id)                                as user_id,
    min(e.event_ts)                                     as started_at,
    cast(min(e.event_ts) as date)                       as session_date,
    date_diff('second', min(e.event_ts), max(e.event_ts)) as duration_seconds,
    count(*)                                            as events,
    bool_or(e.event_type = 'page_view')                 as viewed,
    bool_or(e.event_type = 'add_to_cart')               as added_to_cart,
    bool_or(e.event_type = 'order_placed')              as ordered,
    coalesce(any_value(o.paid), false)                  as paid
from events as e
left join orders as o using (session_id)
group by e.session_id
