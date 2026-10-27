-- Fails when a source is assigned another provider's AQI scale.

select *
from {{ ref('mart_city_air_quality') }}
where (source = 'openweather' and aqi_scale <> 'openweather-1-5')
   or (source = 'waqi' and aqi_scale <> 'us-epa-0-500')
   or source not in ('openweather', 'waqi')
