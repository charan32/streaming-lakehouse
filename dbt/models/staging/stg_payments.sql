select
    event_id,
    json_extract_string(payload, '$.order_id')                  as order_id,
    user_id,
    event_ts                                                    as paid_at,
    cast(json_extract(payload, '$.amount') as decimal(12, 2))   as amount,
    json_extract_string(payload, '$.method')                    as payment_method,
    json_extract_string(payload, '$.status')                    as payment_status
from {{ ref('stg_events') }}
where event_type = 'payment'
