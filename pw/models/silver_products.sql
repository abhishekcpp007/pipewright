-- config: materialized=incremental, unique_key=version_key, tags=silver
select
  cast(product_id as bigint)                        as product_id,
  cast(product_id as varchar) || '|' || updated_at  as version_key,
  upper(trim(sku))                                  as sku,
  nullif(trim(title), '')                           as title,
  lower(trim(category))                             as category,
  cast(price_paise as bigint)                       as price_paise,
  cast(is_active as boolean)                        as is_active,
  cast(updated_at as timestamp)                     as updated_at,
  ingest_date
from {{ ref('bronze_products') }}
{% if is_incremental() %}
where ingest_date = date '{{ batch_date }}'
{% endif %}
qualify row_number() over (partition by product_id, updated_at order by ingest_date desc) = 1
