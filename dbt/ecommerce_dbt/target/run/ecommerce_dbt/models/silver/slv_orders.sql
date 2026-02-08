
  
    

  create  table "ecommerce_db"."analytics_silver"."slv_orders__dbt_tmp"
  
  
    as
  
  (
    -- SILVER LAYER: Orders
-- Cleaned, validated, standardized order data
-- Type casting, null handling, deduplication



with source as (
    select * from "ecommerce_db"."analytics_bronze"."brz_raw_orders"
),

cleaned as (
    select
        -- Order identifiers
        order_id,
        package_id,
        tracking_number,
        
        -- Dates - standardized casting
        cast(order_date as timestamp) as order_date,
        cast(expected_delivery_date as timestamp) as expected_delivery_date,
        cast(actual_delivery_date as timestamp) as actual_delivery_date,
        cast(order_completed_date as timestamp) as order_completed_date,
        cast(payment_date as timestamp) as payment_date,
        
        -- Order status - cleaned
        trim(order_status) as order_status,
        trim(order_type) as order_type,
        trim(return_status) as return_status,
        
        -- Product info - cleaned
        trim(product_sku) as product_sku,
        trim(product_name) as product_name,
        trim(variant_sku) as variant_sku,
        trim(variant_name) as variant_name,
        coalesce(cast("cân_năng_san_phâm" as numeric), 0) as product_weight,
        
        -- Pricing - validated numerics
        coalesce(cast(original_price as numeric), 0) as original_price,
        coalesce(cast("gia_ưu_đai" as numeric), 0) as discount_price,
        coalesce(cast(quantity as integer), 1) as quantity,
        coalesce(cast(total_product_price as numeric), 0) as total_product_price,
        coalesce(cast(order_total_vnd as numeric), 0) as order_total_vnd,
        
        -- Discounts - validated
        coalesce(cast(seller_discount as numeric), 0) as seller_discount,
        coalesce(cast("đươc_shopee_trơ_gia" as numeric), 0) as shopee_discount,
        coalesce(cast(shop_voucher as numeric), 0) as shop_voucher,
        coalesce(cast(shopee_voucher as numeric), 0) as shopee_voucher,
        coalesce(cast(coins_cashback as numeric), 0) as coins_cashback,
        
        -- Shipping - cleaned
        trim(shipping_carrier) as shipping_carrier,
        trim(shipping_method) as shipping_method,
        coalesce(cast("phi_vân_chuyên_dư_kiên" as numeric), 0) as shipping_fee_estimated,
        coalesce(cast("phi_vân_chuyên_ma_ngươi_mua_tra" as numeric), 0) as shipping_fee_paid,
        coalesce(cast(shipping_subsidy as numeric), 0) as shipping_subsidy,
        
        -- Customer info - cleaned
        COALESCE(NULLIF(trim(buyer_username), ''), 'guest_' || order_id) as buyer_username,
        trim(recipient_name) as recipient_name,
        trim(phone_number) as phone_number,
        trim(province) as province,
        trim(district) as district,
        trim(ward) as ward,
        trim(shipping_address) as shipping_address,
        coalesce(trim(country), 'VN') as country,
        
        -- Payment - cleaned
        trim(payment_method) as payment_method,
        coalesce(cast(total_paid as numeric), 0) as total_paid,
        
        -- Fees - validated
        coalesce(cast(fixed_fee as numeric), 0) as fixed_fee,
        coalesce(cast(service_fee as numeric), 0) as service_fee,
        coalesce(cast(payment_fee as numeric), 0) as payment_fee,
        coalesce(cast(deposit as numeric), 0) as deposit,
        
        -- Flags
        case when is_bestseller in ('Y', 'Yes', '1', 'true') then true else false end as is_bestseller,
        buyer_review,
        note,
        
        -- Source metadata
        source_file,
        loaded_at as source_loaded_at,
        data_source,
        _source_system,
        
        -- Calculated fields
        coalesce(cast(seller_discount as numeric), 0) + 
        coalesce(cast("đươc_shopee_trơ_gia" as numeric), 0) + 
        coalesce(cast(shop_voucher as numeric), 0) + 
        coalesce(cast(shopee_voucher as numeric), 0) as total_discount,
        
        -- Silver layer metadata
        current_timestamp as _silver_loaded_at,
        
        -- Row number for deduplication
        row_number() over (
            partition by order_id, product_name 
            order by loaded_at desc
        ) as _row_num
        
    from source
    where order_id is not null
)

-- Keep only the latest record for each order+product
select * from cleaned
where _row_num = 1
  );
  