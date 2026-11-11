{{ config(severity='warn') }}

-- Current prices outside a plausible range per litre or kg, which are usually
-- placeholders or typos. Normal prices run from about 0.55 (GPL) to 3.25.
select station_id, fuel_description, self_service, price
from {{ ref('current_prices') }}
where price < 0.5 or price > 4
