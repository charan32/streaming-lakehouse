select
    json_extract_string(payload, '$.order_id')                  as order_id,
    event_id,
    session_id,
    user_id,
    event_ts                                                    as ordered_at,
    event_date                                                  as order_date,
    cast(json_extract(payload, '$.total') as decimal(12, 2))    as order_total,
    json_extract_string(payload, '$.currency')                  as currency,
    json_array_length(payload, '$.items')                       as line_count
from {{ ref('stg_events') }}
where event_type = 'order_placed'
