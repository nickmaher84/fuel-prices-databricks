-- The latest version of every station ever seen. is_active marks stations in
-- the most recent file.
with stations as (
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
)

select
    stations.station_id,
    stations.station_name,
    stations.station_type,
    stations.operator_name,
    stations.brand_name,
    stations.address,
    stations.comune,
    stations.province_code,
    provinces.province_name,
    provinces.region_name,
    provinces.area,
    stations.latitude,
    stations.longitude,
    stations.last_seen_date,
    stations.is_active,
    stations.last_source_file
from stations
left join {{ ref('provinces') }} as provinces
    on stations.province_code = provinces.province_code
