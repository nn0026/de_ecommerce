-- GOLD LAYER: Product Dimension
-- Business-ready product data with performance metrics

{{ config(
    materialized='table',
    schema='gold'
) }}

with products as (
    select * from {{ ref('slv_products') }}
),

-- Calculate product performance
-- FIX: Use total_product_price (line-level revenue) instead of order_total_vnd (order-level)
product_stats as (
    select
        product_name,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_quantity_sold,
        sum(total_product_price) as total_revenue,
        avg(total_product_price) as avg_line_revenue,
        min(order_date) as first_sold_date,
        max(order_date) as last_sold_date
    from {{ ref('slv_orders') }}
    group by product_name
),

final as (
    select
        p.product_id,
        p.product_name,
        p.product_sku,
        p.variant_name,
        p.variant_sku,
        p.main_category,
        
        -- Pricing
        p.original_price,
        p.discounted_price,
        case 
            when p.original_price > 0 
            then round((1 - p.discounted_price / p.original_price) * 100, 2)
            else 0
        end as discount_percentage,
        
        -- Weight
        p.product_weight,
        
        -- Product key
        p.product_key,
        
        -- Performance metrics
        coalesce(s.total_orders, 0) as total_orders,
        coalesce(s.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(s.total_revenue, 0) as total_revenue,
        coalesce(s.avg_line_revenue, 0) as avg_line_revenue,
        s.first_sold_date,
        s.last_sold_date,
        
        -- Product tier based on revenue (adjusted thresholds for 45 products)
        case
            when s.total_revenue >= 3000000 then 'Star Product'       -- Top tier: 3M+ VND
            when s.total_revenue >= 1500000 then 'High Performer'     -- 1.5M - 3M VND
            when s.total_revenue >= 500000 then 'Moderate'            -- 500K - 1.5M VND
            when s.total_revenue > 0 then 'Low Performer'
            else 'No Sales'
        end as product_tier,
        
        -- Velocity
        case 
            when s.total_quantity_sold >= 50 then 'Fast Moving'
            when s.total_quantity_sold >= 20 then 'Medium Moving'
            when s.total_quantity_sold >= 5 then 'Slow Moving'
            else 'Very Slow'
        end as sales_velocity,
        
        -- Metadata
        current_timestamp as _gold_loaded_at
        
    from products p
    left join product_stats s on p.product_name = s.product_name
)

select * from final
