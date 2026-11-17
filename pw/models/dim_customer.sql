-- config: materialized=scd2, unique_key=customer_id, check_cols=city|tier, watermark=updated_at, tags=dim
-- Feeds the type 2 builder with the batch's version stream. The builder owns the surrogate key,
-- the validity window and the late version rule; this query only says which versions arrived.
select customer_id, full_name, email, city, tier, signup_ts, updated_at
from {{ ref('silver_customers') }}
{% if is_incremental() %}
where ingest_date = date '{{ batch_date }}'
{% endif %}
