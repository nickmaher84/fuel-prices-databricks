select
    line_hash,
    cast({{ clean_text('station_id') }} as int) as station_id,
    {{ clean_text('fuel_description') }} as fuel_description,
    try_cast({{ clean_text('price') }} as decimal(9, 3)) as price,
    case {{ clean_text('self_service') }} when '1' then true when '0' then false end as self_service,
    -- Local Italian time with no zone in the source, so keep it zone-less.
    -- There's no try_to_timestamp_ntz; parsing and casting keeps the wall-clock
    -- time as long as the session time zone has no DST gaps (the default is UTC).
    cast(coalesce(
        try_to_timestamp({{ clean_text('entry_date') }}, 'dd/MM/yyyy HH:mm:ss'),
        try_to_timestamp({{ clean_text('entry_date') }}, 'yyyy-MM-dd HH:mm:ss'),
        try_to_timestamp({{ clean_text('entry_date') }}, 'dd/MM/yyyy')
    ) as timestamp_ntz) as entry_date,
    extraction_date,
    source_file,
    line_number,
    loaded_at
from {{ source('bronze', 'bronze_prices') }}
