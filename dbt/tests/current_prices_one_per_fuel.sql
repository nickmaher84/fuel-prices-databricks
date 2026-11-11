-- Each station should have at most one current price per fuel and service type.
select station_id, fuel_description, self_service, count(*) as prices
from {{ ref('current_prices') }}
group by station_id, fuel_description, self_service
having count(*) > 1
