-- config: materialized=incremental, unique_key=order_id, watermark=updated_at, tags=gold
-- The order grain fact. The join to dim_customer is an ASOF join on the order timestamp, so a
-- row carries the city and tier the customer had when they ordered, not the ones they have now.
-- Join to is_current instead and last month's revenue by city changes every time somebody moves.
with o as (
  select * from {{ ref('silver_orders') }}
  {% if is_incremental() %}
  where ingest_date = date '{{ batch_date }}'
  {% endif %}
),
items as (
  select order_id,
         count(*)                as item_count,
         sum(qty)                as units,
         sum(line_paise)         as gross_paise
  from {{ ref('silver_order_items') }}
  where order_id in (select order_id from o)
  group by order_id
),
pays as (
  select order_id,
         sum(amount_paise)                       as paid_paise,
         count(*)                                as payment_count,
         any_value(method order by updated_at desc) as payment_method
  from {{ ref('silver_payments') }}
  where order_id in (select order_id from o)
  group by order_id
),
current_customer as (
  select customer_id, customer_sk, city, tier
  from {{ ref('dim_customer') }}
  where is_current
)
select
  o.order_id,
  o.order_date,
  o.order_ts,
  o.customer_id,
  coalesce(pit.customer_sk, cur.customer_sk)              as customer_sk,
  coalesce(pit.city, cur.city, 'unknown')                 as customer_city_at_order,
  coalesce(pit.tier, cur.tier, 'unknown')                 as customer_tier_at_order,
  pit.customer_sk is null                                 as attribution_fell_back,
  o.channel,
  o.status,
  o.promo_code,
  coalesce(i.item_count, 0)                               as item_count,
  coalesce(i.units, 0)                                    as units,
  coalesce(i.gross_paise, 0)                              as gross_paise,
  coalesce(p.paid_paise, 0)                               as paid_paise,
  coalesce(p.payment_count, 0)                            as payment_count,
  p.payment_method,
  o.status in ('paid', 'shipped', 'delivered')            as is_revenue,
  o.is_late_arriving,
  o.updated_at,
  o.ingest_date
from o
left join items i on i.order_id = o.order_id
left join pays  p on p.order_id = o.order_id
left join current_customer cur on cur.customer_id = o.customer_id
asof left join {{ ref('dim_customer') }} pit
  on pit.customer_id = o.customer_id and o.order_ts >= pit.valid_from
