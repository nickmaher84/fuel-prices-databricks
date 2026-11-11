-- One row per distinct version of a station, with the span of extraction dates
-- it appeared in. Lines with the same hash have the same values. Incremental runs
-- only read newly loaded rows, and widen the span of any line that has been seen
-- before.
with new_rows as (
    select
        line_hash,
        any_value(station_id) as station_id,
        any_value(station_name) as station_name,
        any_value(station_type) as station_type,
        any_value(operator_name) as operator_name,
        any_value(brand_name) as brand_name,
        any_value(address) as address,
        any_value(comune) as comune,
        any_value(province_code) as province_code,
        any_value(latitude) as latitude,
        any_value(longitude) as longitude,
        min(extraction_date) as min_extraction_date,
        max(extraction_date) as max_extraction_date,
        min_by(source_file, extraction_date) as first_source_file,
        max_by(source_file, extraction_date) as last_source_file,
        max(loaded_at) as last_loaded_at
    from {{ ref('stg_stations') }}
    {% if is_incremental() %}
    where loaded_at > (
        select coalesce(max(last_loaded_at), timestamp '1900-01-01') from {{ this }}
    )
    {% endif %}
    group by line_hash
)

{% if is_incremental() %}
select
    new_rows.line_hash,
    new_rows.station_id,
    new_rows.station_name,
    new_rows.station_type,
    new_rows.operator_name,
    new_rows.brand_name,
    new_rows.address,
    new_rows.comune,
    new_rows.province_code,
    new_rows.latitude,
    new_rows.longitude,
    least(new_rows.min_extraction_date, existing.min_extraction_date) as min_extraction_date,
    greatest(new_rows.max_extraction_date, existing.max_extraction_date) as max_extraction_date,
    case
        when existing.min_extraction_date is null
          or new_rows.min_extraction_date < existing.min_extraction_date
        then new_rows.first_source_file
        else existing.first_source_file
    end as first_source_file,
    case
        when existing.max_extraction_date is null
          or new_rows.max_extraction_date > existing.max_extraction_date
        then new_rows.last_source_file
        else existing.last_source_file
    end as last_source_file,
    greatest(new_rows.last_loaded_at, existing.last_loaded_at) as last_loaded_at
from new_rows
left join {{ this }} as existing
    on new_rows.line_hash = existing.line_hash
{% else %}
select * from new_rows
{% endif %}
