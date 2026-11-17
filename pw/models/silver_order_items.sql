-- config: materialized=incremental, unique_key=order_item_id, tags=silver
-- Items with a quantity of zero are dropped and kept in the quarantine model.
--
-- An item whose order has not arrived yet is NOT dropped. That cost me the first version of
-- this model: the order row lands in the batch where its status last changed, the items land
-- in the batch where the order was placed, so a referential filter at ingest time throws away
-- a legitimate item and nothing ever reconsiders it. Under the polling extract that quietly
-- deleted 92 percent of the revenue. The flag below records what was known at ingest time and
-- the quarantine model decides, with a grace window, what is really an orphan.
select
  cast(i.order_item_id as bigint)   as order_item_id,
  cast(i.order_id as bigint)        as order_id,
  cast(i.product_id as bigint)      as product_id,
  cast(i.qty as integer)            as qty,
  cast(i.unit_price_paise as bigint) as unit_price_paise,
  cast(i.qty as bigint) * cast(i.unit_price_paise as bigint) as line_paise,
  cast(i.updated_at as timestamp)   as updated_at,
  i.ingest_date,
  exists (select 1 from {{ ref('bronze_orders') }} o
          where cast(o.order_id as bigint) = cast(i.order_id as bigint)) as order_known
from {{ ref('bronze_order_items') }} i
where i.qty > 0
{% if is_incremental() %}
  and i.ingest_date = date '{{ batch_date }}'
{% endif %}
qualify row_number() over (partition by i.order_item_id order by i.ingest_date desc) = 1
