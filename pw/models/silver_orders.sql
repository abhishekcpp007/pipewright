-- config: materialized=incremental, unique_key=order_id, watermark=updated_at, tags=silver
-- One row per order, holding the newest state the warehouse has seen. The merge downstream
-- refuses a row whose updated_at is older than the one already stored, which is what keeps a
-- lagging replica from walking an order backwards from delivered to placed.
with first_seen as (
  -- When the order first landed in the lake, not when this version of it landed. The merge
  -- replaces the stored row, so this cannot be carried forward through it: bronze keeps every
  -- version and is the only place the first arrival still exists.
  select cast(order_id as bigint) as order_id, min(ingest_date) as first_seen_date
  from {{ ref('bronze_orders') }}
  group by 1
),
latest as (
  select
    cast(order_id as bigint)                     as order_id,
    cast(customer_id as bigint)                  as customer_id,
    lower(trim(status))                          as status,
    lower(trim(channel))                         as channel,
    cast(order_ts as timestamp)                  as order_ts,
    cast(order_ts as date)                       as order_date,
    cast(updated_at as timestamp)                as updated_at,
    {{ safe_col('bronze_orders', 'promo_code', 'varchar') }} as promo_code,
    ingest_date
  from {{ ref('bronze_orders') }}
  {% if is_incremental() %}
  where ingest_date = date '{{ batch_date }}'
  {% endif %}
  qualify row_number() over (partition by order_id order by updated_at desc, ingest_date desc) = 1
)
select l.*, f.first_seen_date,
       l.order_date < f.first_seen_date as is_late_arriving
from latest l
join first_seen f using (order_id)
