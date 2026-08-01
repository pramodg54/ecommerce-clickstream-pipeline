SELECT DISTINCT
    traffic_source AS source
FROM {{ ref('stg_ga_sessions') }}