-- Prices from the most recent file.
select
    station_id,
    fuel_description,
    self_service,
    price,
    entry_date,
    max_extraction_date as extraction_date
from {{ ref('price_changes') }}
where max_extraction_date = (select max(max_extraction_date) from {{ ref('price_changes') }})
qualify row_number() over (
    partition by station_id, fuel_description, self_service
    order by entry_date desc, line_hash
) = 1
