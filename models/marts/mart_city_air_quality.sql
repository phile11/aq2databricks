-- One row per city, source and measurement hour, with the AQI scale preserved.

with hourly_air as (
    select
        city, source, aqi_scale,
        date_trunc('hour', measured_at_utc) as measured_at_utc,
        avg(aqi_value) as aqi_value,
        avg(pm25_ugm3) as pm25_ugm3,
        max(station_name) as station_name,
        max(fetched_at_utc) as latest_fetch_at_utc
    from {{ ref('stg_air_quality') }}
    group by city, source, aqi_scale, date_trunc('hour', measured_at_utc)
),
hourly_weather as (
    select
        city,
        date_trunc('hour', measured_at_utc) as measured_at_utc,
        avg(temp_c) as temp_c,
        max(conditions) as conditions
    from {{ ref('stg_weather') }}
    group by city, date_trunc('hour', measured_at_utc)
)

select
    concat(a.city, '|', a.source, '|', cast(a.measured_at_utc as string))
        as city_source_measurement_hour,
    a.city, a.source, a.aqi_scale, a.measured_at_utc,
    a.aqi_value, a.pm25_ugm3, a.station_name,
    w.temp_c, w.conditions,
    -- EPA 2024 PM2.5 breakpoints. A single snapshot is a descriptive band,
    -- not a regulatory 24-hour AQI.
    case
        when a.pm25_ugm3 is null then 'not_available'
        when a.pm25_ugm3 <= 9.0 then 'good'
        when a.pm25_ugm3 <= 35.4 then 'moderate'
        when a.pm25_ugm3 <= 55.4 then 'unhealthy_for_sensitive_groups'
        when a.pm25_ugm3 <= 125.4 then 'unhealthy'
        when a.pm25_ugm3 <= 225.4 then 'very_unhealthy'
        else 'hazardous'
    end as pm25_category
from hourly_air a
left join hourly_weather w
    on a.city = w.city and a.measured_at_utc = w.measured_at_utc