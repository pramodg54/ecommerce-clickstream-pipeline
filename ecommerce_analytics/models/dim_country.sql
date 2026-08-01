SELECT DISTINCT
    country
FROM {{ ref('stg_ga_sessions') }}