-- config: materialized=table, tags=gold
select
  f.customer_id,
  d.customer_sk                                      as current_customer_sk,
  d.city                                             as current_city,
  d.tier                                             as current_tier,
  min(f.order_date)                                  as first_order_date,
  max(f.order_date)                                  as last_order_date,
  count(*)                                           as orders,
  sum(f.gross_paise) filter (where f.is_revenue)     as lifetime_gross_paise,
  sum(f.paid_paise)                                  as lifetime_paid_paise,
  count(distinct f.customer_city_at_order)           as cities_ordered_from,
  count(distinct f.promo_code)                       as promos_used
from {{ ref('fct_orders') }} f
left join {{ ref('dim_customer') }} d
  on d.customer_id = f.customer_id and d.is_current
group by f.customer_id, d.customer_sk, d.city, d.tier
