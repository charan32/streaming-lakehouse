-- One row per event. Removes duplicates caused by producer retries and by the
-- ingestor replaying a batch after a crash (at-least-once delivery).
with source as (
    select * from {{ source('bronze', 'events') }}
),

ranked as (
    select
        *,
        row_number() over (partition by event_id order by ingested_at, source_offset) as copy_number
    from source
)

select
    event_id,
    event_type,
    event_ts,
    cast(event_ts as date)                             as event_date,
    user_id,
    session_id,
    schema_version,
    payload,
    ingested_at,
    -- Late events: how far behind real time the event arrived.
    greatest(date_diff('second', event_ts, ingested_at), 0) as arrival_delay_seconds
from ranked
where copy_number = 1
