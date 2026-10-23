select
    event_id,
    session_id,
    user_id,
    event_ts,
    event_date,
    json_extract_string(payload, '$.page')       as page,
    json_extract_string(payload, '$.product_id') as product_id
from {{ ref('stg_events') }}
where event_type = 'page_view'
