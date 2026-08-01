SELECT *
FROM read_parquet(
    '/opt/project/data/silver/stg_sessions/visit_date=*/*.parquet',
    hive_partitioning = true
)