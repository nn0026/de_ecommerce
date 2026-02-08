
  
    

  create  table "ecommerce_db"."analytics_gold"."agg_product_performance__dbt_tmp"
  
  
    as
  
  (
    -- GOLD LAYER: Product Performance Aggregate
-- Product-level aggregated metrics



with product_orders as (
    select
        product_name,
        product_id,
        main_category,
        
        -- Order counts
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed then order_id end) as completed_orders,
        count(distinct case when is_returned then order_id end) as returned_orders,
        
        -- Customer counts
        count(distinct buyer_username) as unique_customers,
        count(distinct case when new_vs_repeat = 'New' then buyer_username end) as new_customers,
        
        -- Quantity
        sum(quantity) as total_quantity_sold,
        avg(quantity) as avg_quantity_per_order,
        
        -- Revenue
        sum(order_total_vnd) as total_revenue,
        sum(net_revenue) as total_net_revenue,
        avg(order_total_vnd) as avg_order_value,
        
        -- Pricing
        avg(original_price) as avg_original_price,
        avg(discount_price) as avg_discount_price,
        sum(total_discount) as total_discount_given,
        
        -- Time metrics
        min(order_date) as first_sold_date,
        max(order_date) as last_sold_date,
        current_date - max(order_date)::date as days_since_last_sale,
        
        -- Geography
        mode() within group (order by region) as top_region,
        
        -- Sale events
        count(distinct case when is_double_day_sale then order_id end) as sale_event_orders
        
    from "ecommerce_db"."analytics_gold"."fct_orders"
    where product_name is not null
    group by 1, 2, 3
)

select
    product_name,
    product_id,
    main_category,
    
    -- Order metrics
    total_orders,
    completed_orders,
    returned_orders,
    
    -- Customer metrics
    unique_customers,
    new_customers,
    
    -- Quantity metrics
    total_quantity_sold,
    avg_quantity_per_order,
    
    -- Revenue metrics
    total_revenue,
    total_net_revenue,
    avg_order_value,
    
    -- Pricing metrics
    avg_original_price,
    avg_discount_price,
    total_discount_given,
    
    -- Discount percentage
    case 
        when avg_original_price > 0 
        then round((1 - avg_discount_price / avg_original_price) * 100, 2)
        else 0 
    end as avg_discount_pct,
    
    -- Time metrics
    first_sold_date,
    last_sold_date,
    days_since_last_sale,
    
    -- Selling period (days)
    last_sold_date::date - first_sold_date::date as selling_period_days,
    
    -- Sales velocity (units per day)
    case 
        when last_sold_date::date - first_sold_date::date > 0 
        then round(total_quantity_sold * 1.0 / (last_sold_date::date - first_sold_date::date + 1), 2)
        else total_quantity_sold
    end as daily_sales_velocity,
    
    -- Top region
    top_region,
    
    -- Sale event performance
    sale_event_orders,
    case 
        when total_orders > 0 
        then round(sale_event_orders * 100.0 / total_orders, 2)
        else 0 
    end as sale_event_order_pct,
    
    -- Return rate
    case 
        when total_orders > 0 
        then round(returned_orders * 100.0 / total_orders, 2)
        else 0 
    end as return_rate_pct,
    
    -- Customer acquisition
    case 
        when unique_customers > 0 
        then round(new_customers * 100.0 / unique_customers, 2)
        else 0 
    end as new_customer_pct,
    
    -- Product tier
    case
        when total_revenue >= 10000000 then 'Star Product'
        when total_revenue >= 5000000 then 'High Performer'
        when total_revenue >= 1000000 then 'Moderate'
        when total_revenue > 0 then 'Low Performer'
        else 'No Sales'
    end as product_tier,
    
    -- Sales velocity category
    case 
        when total_quantity_sold >= 50 then 'Fast Moving'
        when total_quantity_sold >= 20 then 'Medium Moving'
        when total_quantity_sold >= 5 then 'Slow Moving'
        else 'Very Slow'
    end as sales_velocity_category,
    
    -- Metadata
    current_timestamp as _gold_loaded_at
    
from product_orders
order by total_revenue desc
  );
  