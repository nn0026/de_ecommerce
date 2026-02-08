-- GOLD LAYER: Product Dimension
-- Business-ready product data with performance metrics



with products as (
    select * from "ecommerce_db"."analytics_silver"."slv_products"
),

-- Calculate product performance
product_stats as (
    select
        product_name,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_quantity_sold,
        sum(order_total_vnd) as total_revenue,
        avg(order_total_vnd) as avg_order_value,
        min(order_date) as first_sold_date,
        max(order_date) as last_sold_date
    from "ecommerce_db"."analytics_silver"."slv_orders"
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
        coalesce(s.avg_order_value, 0) as avg_order_value,
        s.first_sold_date,
        s.last_sold_date,
        
        -- Product tier based on revenue
        case
            when s.total_revenue >= 10000000 then 'Star Product'
            when s.total_revenue >= 5000000 then 'High Performer'
            when s.total_revenue >= 1000000 then 'Moderate'
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