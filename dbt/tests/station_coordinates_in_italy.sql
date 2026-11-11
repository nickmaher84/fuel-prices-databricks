{{ config(severity='warn') }}

select station_id, latitude, longitude
from {{ ref('current_stations') }}
where latitude not between 35 and 48
   or longitude not between 6 and 19
