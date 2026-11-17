-- config: materialized=table, tags=silver
-- Rows the silver models refused, with the reason. Dropping bad rows without keeping them is
-- how a pipeline loses money quietly: the daily total is wrong and nothing says so.
--
-- Rebuilt whole every run rather than merged, because one of the reasons expires. An item
-- whose order has not arrived yet is pending, not orphaned, and it only becomes an orphan
-- after the grace window below. A merge would have frozen the first answer.
with watermark as (
  select max(ingest_date) as latest from {{ ref('bronze_order_items') }}
),
items as (
  select 'order_items' as source_table, cast(i.order_item_id as varchar) as source_key,
         case when i.qty <= 0 then 'non_positive_qty' else 'orphan_order_id' end as reason,
         cast(i.qty as bigint) * cast(i.unit_price_paise as bigint) as amount_paise,
         i.ingest_date
  from {{ ref('bronze_order_items') }} i, watermark w
  where i.qty <= 0
     or (not exists (select 1 from {{ ref('bronze_orders') }} o
                     where cast(o.order_id as bigint) = cast(i.order_id as bigint))
         and date_diff('day', i.ingest_date, w.latest) >= 2)
),
pays as (
  select 'payments' as source_table, cast(payment_id as varchar) as source_key,
         case when amount_paise = 0 then 'zero_amount' else 'negative_amount' end as reason,
         cast(amount_paise as bigint) as amount_paise, ingest_date
  from {{ ref('bronze_payments') }}
  where amount_paise <= 0
),
custs as (
  select 'customers' as source_table, cast(customer_id as varchar) as source_key,
         'unparseable_email' as reason, null::bigint as amount_paise, ingest_date
  from {{ ref('bronze_customers') }}
  where not (email like '%@%.%')
),
all_bad as (select * from items union all select * from pays union all select * from custs)
select source_table || '|' || source_key || '|' || reason as quarantine_key, *
from all_bad
qualify row_number() over (partition by source_table, source_key, reason
                           order by ingest_date desc) = 1
