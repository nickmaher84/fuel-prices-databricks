-- The price for each station, fuel and service type on each day. A price takes
-- effect from the station's submission (entry_date) and holds until its next one;
-- each day gets the price in force at 08:00, matching the 'prezzo alle 8' files.
-- Each series ends on the last day its fuel appeared in a file.
--
-- Days between files are filled in from the submissions we've seen, so a change
-- made and reverted between two files we have won't show.
--
-- Nothing is stored, so filter by station (and ideally dates) when querying.
with submissions as (
    select
        station_id,
        fuel_description,
        self_service,
        entry_date,
        any_value(price) as price,
        max(max_extraction_date) as last_seen
    from {{ ref('price_changes') }}
    group by all
),

ranges as (
    select
        *,
        -- The first 08:00 at or after the submission.
        date(entry_date + interval 16 hours) as first_day,
        coalesce(
            date(
                lead(entry_date) over (
                    partition by station_id, fuel_description, self_service
                    order by entry_date
                ) + interval 16 hours
            ) - 1,
            max(last_seen) over (partition by station_id, fuel_description, self_service)
        ) as last_day
    from submissions
)

select
    station_id,
    fuel_description,
    self_service,
    price_date,
    price,
    entry_date
from ranges
lateral view explode(sequence(first_day, last_day, interval 1 day)) as price_date
where first_day <= last_day
