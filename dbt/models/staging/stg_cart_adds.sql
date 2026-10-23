select
    event_id,
    session_id,
    user_id,
    event_ts,
    event_date,
    json_extract_string(payload, '$.product_id')                    as product_id,
    cast(json_extract(payload, '$.quantity') as integer)            as quantity,
    cast(json_extract(payload, '$.unit_price') as decimal(10, 2))   as unit_price
from {{ ref('stg_events') }}
where event_type = 'add_to_cart'
