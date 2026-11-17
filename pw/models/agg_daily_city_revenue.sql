-- config: materialized=table, tags=gold
-- Rebuilt whole every run. It is small, and a rebuild means a late arriving order from three
-- days ago lands in the right day's row instead of today's.
select
  order_date,
  customer_city_at_order                                       as city,
  count(*)                                                     as orders,
  count(*) filter (where status = 'delivered')                  as delivered_orders,
  count(*) filter (where status = 'cancelled')                  as cancelled_orders,
  count(*) filter (where is_late_arriving)                      as late_arriving_orders,
  sum(gross_paise) filter (where is_revenue)                    as gross_paise,
  sum(paid_paise)                                               as paid_paise,
  round(avg(gross_paise) filter (where is_revenue), 1)          as avg_order_paise,
  count(distinct customer_id)                                   as customers
from {{ ref('fct_orders') }}
group by order_date, customer_city_at_order
