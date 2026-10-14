/* Locations from config/locations.yaml (mirrored into bronze.locations by the ingester). */
select location_id, name, state, latitude, longitude, timezone, sort_order
from {{ source('bronze', 'locations') }}
