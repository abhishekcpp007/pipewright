-- config: materialized=scd2, unique_key=product_id, check_cols=price_paise|category|is_active, watermark=updated_at, tags=dim
select product_id, sku, title, category, price_paise, is_active, updated_at
from {{ ref('silver_products') }}
{% if is_incremental() %}
where ingest_date = date '{{ batch_date }}'
{% endif %}
