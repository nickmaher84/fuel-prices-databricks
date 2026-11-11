-- One row per distinct price line, with the span of extraction dates it
-- appeared in. Incremental runs only read newly loaded rows, and widen the span
-- of any line that has been seen before. Rows without a usable price or entry
-- date are left out, as in the original backend.
with new_rows as (
    select
        line_hash,
        any_value(station_id) as station_id,
        any_value(fuel_description) as fuel_description,
        any_value(self_service) as self_service,
        any_value(price) as price,
        any_value(entry_date) as entry_date,
        min(extraction_date) as min_extraction_date,
        max(extraction_date) as max_extraction_date,
        min_by(source_file, extraction_date) as first_source_file,
        max_by(source_file, extraction_date) as last_source_file,
        max(loaded_at) as last_loaded_at
    from {{ ref('stg_prices') }}
    where price is not null
      and entry_date is not null
    {% if is_incremental() %}
      and loaded_at > (
          select coalesce(max(last_loaded_at), timestamp '1900-01-01') from {{ this }}
      )
    {% endif %}
    group by line_hash
)

{% if is_incremental() %}
select
    new_rows.line_hash,
    new_rows.station_id,
    new_rows.fuel_description,
    new_rows.self_service,
    new_rows.price,
    new_rows.entry_date,
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
