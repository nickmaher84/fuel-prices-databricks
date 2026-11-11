-- The latest version of every station ever seen. is_active marks stations in
-- the most recent file.
select
    station_id,
    station_name,
    station_type,
    operator_name,
    brand_name,
    address,
    upper(comune) as comune,
    province_code,
    latitude,
    longitude,
    max_extraction_date as last_seen_date,
    max_extraction_date = max(max_extraction_date) over () as is_active,
    last_source_file
from {{ ref('station_changes') }}
qualify row_number() over (
    partition by station_id
    order by max_extraction_date desc, min_extraction_date desc, line_hash
) = 1
