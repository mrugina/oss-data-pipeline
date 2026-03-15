-- models/active_flights_total.sql
-- Latest worldwide count of flights currently in the air.
-- Prefers the "active_flights" stream; falls back to the latest sync if not yet configured.

WITH ranked AS (
    SELECT
        data->>'_airbyte_stream'                                  AS stream_name,
        (data->'_airbyte_data'->'pagination'->>'total')::int      AS flights_in_the_air,
        created_at                                                AS synced_at,
        ROW_NUMBER() OVER (
            ORDER BY
                -- prefer active_flights stream, then most recent
                CASE WHEN data->>'_airbyte_stream' = 'active_flights' THEN 0 ELSE 1 END,
                created_at DESC
        ) AS rn
    FROM {{ source('raw_data', 'flights') }}
    WHERE data->'_airbyte_data'->'pagination' IS NOT NULL
)

SELECT
    flights_in_the_air,
    synced_at
FROM ranked
WHERE rn = 1

