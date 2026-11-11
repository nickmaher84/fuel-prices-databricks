{{ config(severity='warn') }}

-- Recently loaded price rows whose price or entry date couldn't be parsed.
-- These are left out of price_changes.
select source_file, line_number, price, entry_date
from {{ ref('stg_prices') }}
where loaded_at >= current_date() - interval 1 day
  and (price is null or entry_date is null)
