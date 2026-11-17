-- config: materialized=incremental, unique_key=version_key, tags=silver
-- The full version stream for a customer, one row per (customer_id, updated_at), cleaned and
-- typed. Nothing is collapsed to a latest row here: the type 2 dimension downstream needs the
-- history, and a model that threw it away could not be rebuilt without re-reading the source.
select
  cast(customer_id as bigint)                               as customer_id,
  cast(customer_id as varchar) || '|' || updated_at         as version_key,
  nullif(trim(full_name), '')                               as full_name,
  lower(trim(email))                                        as email,
  email like '%@%.%'                                        as email_valid,
  coalesce(nullif(trim(city), ''), 'unknown')               as city,
  lower(trim(tier))                                         as tier,
  cast(signup_ts as timestamp)                              as signup_ts,
  cast(updated_at as timestamp)                             as updated_at,
  ingest_date
from {{ ref('bronze_customers') }}
{% if is_incremental() %}
where ingest_date = date '{{ batch_date }}'
{% endif %}
-- The extract is inclusive on the watermark, so the boundary row arrives twice. Same row,
-- same values, so either copy will do.
qualify row_number() over (partition by customer_id, updated_at order by ingest_date desc) = 1
