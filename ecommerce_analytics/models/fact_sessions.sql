{{ config(materialized='table') }}

SELECT

    session_id,
    user_id,
    visit_id,
    visit_number,
    visit_start_time,
    visit_date,

    channel_grouping,
    traffic_source,
    traffic_medium,
    campaign,
    keyword,
    ad_content,
    referral_path,
    adwords_click_info,

    device_category,
    browser,
    browser_version,
    operating_system,
    operating_system_version,
    mobile_brand,
    mobile_model,
    language,
    screen_resolution,
    is_mobile,

    continent,
    sub_continent,
    country,
    region,
    metro,
    city,
    network_domain,
    latitude,
    longitude,

    page_views,
    hits,
    transactions,
    transaction_revenue,
    bounces,
    new_visits,
    time_on_site,

    social_engagement_type,

    etl_loaded_timestamp,
    pipeline_name,
    layer,
    job_version,
    created_by,

    CASE
        WHEN transactions > 0 THEN TRUE
        ELSE FALSE
    END AS is_transaction

FROM {{ ref('stg_ga_sessions') }}