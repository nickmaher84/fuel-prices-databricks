select
    line_hash,
    cast({{ clean_text('station_id') }} as int) as station_id,
    {{ clean_text('station_name') }} as station_name,
    {{ clean_text('station_type') }} as station_type,
    {{ clean_text('operator_name') }} as operator_name,
    {{ clean_text('brand_name') }} as brand_name,
    {{ clean_text('address') }} as address,
    {{ clean_text('comune') }} as comune,
    {{ clean_text('province_code') }} as province_code,
    try_cast({{ clean_text('latitude') }} as double) as latitude,
    try_cast({{ clean_text('longitude') }} as double) as longitude,
    extraction_date,
    source_file,
    line_number,
    loaded_at
from {{ source('bronze', 'bronze_stations') }}
