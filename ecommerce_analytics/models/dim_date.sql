{{ config(materialized='table') }}

SELECT DISTINCT

    visit_date,

    STRPTIME(CAST(visit_date AS VARCHAR), '%Y%m%d') AS full_date,

    YEAR(STRPTIME(CAST(visit_date AS VARCHAR), '%Y%m%d')) AS year,

    MONTH(STRPTIME(CAST(visit_date AS VARCHAR), '%Y%m%d')) AS month,

    DAY(STRPTIME(CAST(visit_date AS VARCHAR), '%Y%m%d')) AS day

FROM {{ ref('stg_ga_sessions') }}