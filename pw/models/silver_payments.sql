-- config: materialized=incremental, unique_key=payment_id, watermark=updated_at, tags=silver
-- The source table has no unique constraint, so the gateway's retries are in here twice, and
-- a few of those retries disagree about the amount. Last write wins, with the amount as the
-- tie break, so the same input always collapses to the same row no matter what order the
-- files are read in. Non positive amounts are refunds booked as captures: dropped, quarantined.
select
  cast(payment_id as bigint)     as payment_id,
  cast(order_id as bigint)       as order_id,
  cast(amount_paise as bigint)   as amount_paise,
  lower(trim(method))            as method,
  lower(trim(status))            as status,
  cast(paid_at as timestamp)     as paid_at,
  cast(updated_at as timestamp)  as updated_at,
  ingest_date
from {{ ref('bronze_payments') }}
where amount_paise > 0
{% if is_incremental() %}
  and ingest_date = date '{{ batch_date }}'
{% endif %}
qualify row_number() over (partition by payment_id
                           order by updated_at desc, amount_paise desc, ingest_date desc) = 1
