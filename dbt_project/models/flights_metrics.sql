-- models/flights_metrics.sql
-- One row per flight — Superset handles aggregation (COUNT, AVG, etc.)

WITH raw AS (
    SELECT
        id,
        data->>'_airbyte_stream'          AS stream_name,
        data->'_airbyte_data'->'data'     AS flight_array,
        created_at
    FROM {{ source('raw_data', 'flights') }}
    WHERE data->'_airbyte_data'->'data' IS NOT NULL
      AND COALESCE(data->>'_airbyte_stream', '') != 'active_flights'
)

SELECT
    r.id                                              AS raw_id,
    r.stream_name,
    CASE
        WHEN r.stream_name = 'departures_otp' THEN 'departure'
        WHEN r.stream_name = 'arrivals_otp'   THEN 'arrival'
        ELSE 'unknown'
    END                                               AS direction,
    f.value->>'flight_status'                         AS flight_status,
    f.value->'airline'->>'name'                       AS airline,
    (f.value->'departure'->>'delay')::int             AS departure_delay,
    (f.value->'arrival'->>'delay')::int               AS arrival_delay,
    f.value->'departure'->>'airport'                  AS departure_airport,
    f.value->'arrival'->>'airport'                    AS arrival_airport,
    (f.value->>'flight_date')::date                   AS flight_date,
    r.created_at
FROM raw r,
     jsonb_array_elements(r.flight_array) AS f(value)
WHERE
    f.value->'airline'->>'name' IS NOT NULL
    AND f.value->'airline'->>'name' != 'empty'

