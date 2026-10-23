-- Why events were rejected, per day and producer event type. Feeds alerting.
with rejected as (
    select
        cast(ingested_at as date)                         as day,
        coalesce(event_type, '(unparseable)')             as event_type,
        split_part(split_part(errors, chr(10), 1), ':', 1) as failed_field,
        errors
    from {{ source('bronze', 'events_rejected') }}
)

select
    day,
    event_type,
    failed_field,
    count(*)           as rejected_events,
    any_value(errors)  as example_error
from rejected
group by all
order by day, rejected_events desc
