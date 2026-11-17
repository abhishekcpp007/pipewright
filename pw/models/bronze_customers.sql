-- config: materialized=view, tags=bronze
-- Raw landing zone, read straight off Parquet. union_by_name lets a file written before a
-- source column existed sit next to one written after it.
select * from read_parquet('{{ lake }}/bronze/customers/**/*.parquet',
                           union_by_name = true, hive_partitioning = 1)
