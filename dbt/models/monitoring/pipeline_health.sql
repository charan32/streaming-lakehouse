-- Freshness, duplicates and contract quality in one row per ingestion day.
with bronze as (
    select cast(ingested_at as date) as day, count(*) as bronze_rows, count(distinct event_id) as unique_events
    from {{ source('bronze', 'events') }}
    group by 1
),

rejected as (
    select cast(ingested_at as date) as day, count(*) as rejected_rows
    from {{ source('bronze', 'events_rejected') }}
    group by 1
),

lag as (
    select
        cast(ingested_at as date)                                  as day,
        max(ingested_at)                                           as last_ingested_at,
        quantile_cont(arrival_delay_seconds, 0.95)                 as p95_arrival_delay_seconds,
        count(*) filter (where arrival_delay_seconds > 300)        as late_events
    from {{ ref('stg_events') }}
    group by 1
)

select
    b.day,
    b.bronze_rows,
    b.bronze_rows - b.unique_events                                  as duplicates_removed,
    coalesce(r.rejected_rows, 0)                                     as rejected_rows,
    round(coalesce(r.rejected_rows, 0) / (b.bronze_rows + coalesce(r.rejected_rows, 0)), 4) as reject_rate,
    l.last_ingested_at,
    l.p95_arrival_delay_seconds,
    l.late_events
from bronze as b
left join rejected as r using (day)
left join lag as l using (day)
order by b.day
