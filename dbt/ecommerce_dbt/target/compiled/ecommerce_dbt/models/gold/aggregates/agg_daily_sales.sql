-- GOLD LAYER: Daily Sales Aggregate
-- Daily sales metrics for dashboards



with daily_orders as (
    select
        cast(order_date as date) as order_date,
        order_date_key,
        is_double_day_sale,
        sale_event_name,
        
        -- Order counts
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed then order_id end) as completed_orders,
        count(distinct case when is_cancelled then order_id end) as cancelled_orders,
        count(distinct case when is_returned then order_id end) as returned_orders,
        
        -- Customer counts
        count(distinct buyer_username) as unique_customers,
        count(distinct case when new_vs_repeat = 'New' then buyer_username end) as new_customers,
        count(distinct case when new_vs_repeat = 'Repeat' then buyer_username end) as repeat_customers,
        
        -- Product counts
        count(distinct product_name) as unique_products,
        sum(quantity) as total_quantity,
        
        -- Revenue metrics
        sum(order_total_vnd) as gross_revenue,
        sum(net_revenue) as net_revenue,
        sum(total_discount) as total_discount,
        sum(shipping_fee_paid) as total_shipping,
        
        -- Fee breakdown
        sum(fixed_fee) as total_fixed_fee,
        sum(service_fee) as total_service_fee,
        sum(payment_fee) as total_payment_fee,
        
        -- Averages
        avg(order_total_vnd) as avg_order_value,
        avg(quantity) as avg_items_per_order,
        avg(delivery_days) as avg_delivery_days,
        
        -- By order value tier
        count(distinct case when order_value_tier = 'Premium (1M+)' then order_id end) as premium_orders,
        count(distinct case when order_value_tier = 'Micro (<100K)' then order_id end) as micro_orders
        
    from "ecommerce_db"."analytics_gold"."fct_orders"
    group by 1, 2, 3, 4
)

select
    d.*,
    
    -- Completion rate
    case 
        when total_orders > 0 
        then round(completed_orders * 100.0 / total_orders, 2)
        else 0 
    end as completion_rate_pct,
    
    -- Cancellation rate
    case 
        when total_orders > 0 
        then round(cancelled_orders * 100.0 / total_orders, 2)
        else 0 
    end as cancellation_rate_pct,
    
    -- Return rate
    case 
        when total_orders > 0 
        then round(returned_orders * 100.0 / total_orders, 2)
        else 0 
    end as return_rate_pct,
    
    -- New customer rate
    case 
        when unique_customers > 0 
        then round(new_customers * 100.0 / unique_customers, 2)
        else 0 
    end as new_customer_rate_pct,
    
    -- Metadata
    current_timestamp as _gold_loaded_at
    
from daily_orders d
order by order_date desc