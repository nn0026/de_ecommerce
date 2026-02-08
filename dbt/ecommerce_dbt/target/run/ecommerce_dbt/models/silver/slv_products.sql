
  
    

  create  table "ecommerce_db"."analytics_silver"."slv_products__dbt_tmp"
  
  
    as
  
  (
    -- SILVER LAYER: Products
-- Extracted from raw_orders, cleaned and standardized



with source as (
    select * from "ecommerce_db"."analytics_bronze"."brz_raw_orders"
),

-- Extract unique products from orders (one row per product_name, keeping highest price variant)
unique_products as (
    select distinct on (product_name)
        product_name,
        product_sku,
        variant_name,
        variant_sku,
        "cân_năng_san_phâm" as product_weight,
        original_price,
        "gia_ưu_đai" as discount_price,
        _source_system
    from source
    where product_name is not null
    order by product_name, original_price desc
),

cleaned as (
    select
        -- Generate product_id
        row_number() over (order by product_name, coalesce(variant_name, '')) as product_id,
        
        trim(product_name) as product_name,
        trim(product_sku) as product_sku,
        trim(variant_name) as variant_name,
        trim(variant_sku) as variant_sku,
        
        -- Category extraction from product name (Vietnamese keywords)
        case
            when product_name ilike '%áo%' then 'Clothing'
            when product_name ilike '%quần%' then 'Clothing'
            when product_name ilike '%váy%' then 'Clothing'
            when product_name ilike '%giày%' then 'Footwear'
            when product_name ilike '%dép%' then 'Footwear'
            when product_name ilike '%túi%' then 'Bags'
            when product_name ilike '%balo%' then 'Bags'
            when product_name ilike '%đồng hồ%' then 'Watches'
            when product_name ilike '%phụ kiện%' then 'Accessories'
            when product_name ilike '%mỹ phẩm%' then 'Beauty'
            when product_name ilike '%son%' then 'Beauty'
            when product_name ilike '%kem%' then 'Beauty'
            else 'Other'
        end as main_category,
        
        -- Pricing from first occurrence
        coalesce(cast(original_price as numeric), 0) as original_price,
        coalesce(cast(discount_price as numeric), 0) as discounted_price,
        coalesce(cast(product_weight as numeric), 0) as product_weight,
        
        -- Product key for joining
        md5(coalesce(product_name, '') || '|' || coalesce(product_sku, '')) as product_key,
        
        -- Source metadata
        _source_system,
        
        -- Silver layer metadata
        current_timestamp as _silver_loaded_at
        
    from unique_products
)

select * from cleaned
  );
  