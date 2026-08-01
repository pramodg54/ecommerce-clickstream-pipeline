SELECT DISTINCT
    device_category
FROM {{ ref('stg_ga_sessions') }}