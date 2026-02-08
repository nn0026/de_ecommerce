-- SILVER LAYER: Products
-- Extracted from raw_orders, cleaned and standardized

{{ config(
    materialized='table',
    schema='silver'
) }}

with source as (
    select * from {{ ref('brz_raw_orders') }}
),

-- Extract unique products from orders (one row per product_name, keeping highest price variant)
unique_products as (
    select distinct on (product_name)
        product_name,
        product_sku,
        variant_name,
        variant_sku,
        product_weight,
        original_price,
        discount_price,
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
        
        -- Category extraction from product name (Anime/Manga collectibles shop)
        case
            -- Metal Cards / Character Cards
            when product_name ilike '%thẻ nhân phẩm%' then 'Metal Cards'
            when product_name ilike '%metal card%' then 'Metal Cards'
            when product_name ilike '%card%' then 'Metal Cards'
            
            -- Goods / Merchandise by anime
            when product_name ilike '%mha%' or product_name ilike '%học viện anh hùng%' then 'MHA Merchandise'
            when product_name ilike '%jjk%' or product_name ilike '%jujutsu%' then 'JJK Merchandise'
            when product_name ilike '%tokyo revengers%' then 'Tokyo Revengers'
            when product_name ilike '%attack on titan%' or product_name ilike '%aot%' then 'Attack on Titan'
            when product_name ilike '%haikyuu%' then 'Haikyuu Merchandise'
            when product_name ilike '%spy%family%' or product_name ilike '%spy x family%' then 'Spy x Family'
            when product_name ilike '%demon slayer%' or product_name ilike '%kimetsu%' then 'Demon Slayer'
            
            -- Gift/Promo items
            when product_name ilike '%hàng tặng%' or product_name ilike '%không bán%' then 'Gift/Promo Items'
            
            -- General goods
            when product_name ilike '%goods%' then 'Anime Goods'
            when product_name ilike '%figure%' or product_name ilike '%mô hình%' then 'Figures'
            when product_name ilike '%poster%' then 'Posters'
            when product_name ilike '%sticker%' then 'Stickers'
            when product_name ilike '%keychain%' or product_name ilike '%móc khóa%' then 'Keychains'
            
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
