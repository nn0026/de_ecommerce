-- GOLD LAYER: Customer Summary Aggregate
-- Customer-level aggregated metrics with RFM

{{ config(
    materialized='table',
    schema='gold'
) }}

with customer_orders as (
    select
        buyer_username,
        customer_id,
        region,
        
        -- Order counts
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed then order_id end) as completed_orders,
        count(distinct case when is_returned then order_id end) as returned_orders,
        
        -- Quantity
        sum(quantity) as total_items_purchased,
        
        -- Revenue
        sum(order_total_vnd) as total_spent,
        sum(net_revenue) as total_net_revenue,
        avg(order_total_vnd) as avg_order_value,
        
        -- Time metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        current_date - max(order_date)::date as days_since_last_order,
        
        -- Product diversity
        count(distinct product_name) as unique_products_ordered,
        count(distinct main_category) as unique_categories,
        
        -- Shipping preferences
        mode() within group (order by carrier_group) as preferred_carrier,
        mode() within group (order by payment_group) as preferred_payment
        
    from {{ ref('fct_orders') }}
    where buyer_username is not null
    group by 1, 2, 3
),

with_rfm as (
    select
        *,
        
        -- RFM Scores
        ntile(5) over (order by days_since_last_order desc nulls last) as r_score,
        ntile(5) over (order by total_orders asc nulls first) as f_score,
        ntile(5) over (order by total_spent asc nulls first) as m_score
        
    from customer_orders
    where total_orders > 0
)

select
    buyer_username,
    customer_id,
    region,
    
    -- Order metrics
    total_orders,
    completed_orders,
    returned_orders,
    total_items_purchased,
    
    -- Revenue metrics
    total_spent,
    total_net_revenue,
    avg_order_value,
    
    -- Time metrics
    first_order_date,
    last_order_date,
    days_since_last_order,
    
    -- Tenure (days as customer)
    current_date - first_order_date::date as customer_tenure_days,
    
    -- Frequency (orders per month)
    case 
        when current_date - first_order_date::date > 30 
        then round(total_orders * 30.0 / (current_date - first_order_date::date), 2)
        else total_orders
    end as orders_per_month,
    
    -- Product diversity
    unique_products_ordered,
    unique_categories,
    
    -- Preferences
    preferred_carrier,
    preferred_payment,
    
    -- RFM
    r_score,
    f_score,
    m_score,
    r_score * 100 + f_score * 10 + m_score as rfm_score,
    
    -- RFM Segment
    case
        when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
        when r_score >= 4 and f_score >= 3 then 'Loyal Customers'
        when r_score >= 4 and f_score <= 2 then 'Recent Customers'
        when r_score >= 3 and f_score >= 3 and m_score >= 3 then 'Potential Loyalists'
        when r_score <= 2 and f_score >= 4 then 'At Risk'
        when r_score <= 2 and f_score >= 2 then 'Hibernating'
        when r_score <= 2 and f_score <= 2 then 'Lost'
        else 'Other'
    end as customer_segment,
    
    -- Customer lifecycle
    case 
        when total_orders = 1 then 'New'
        when total_orders between 2 and 3 then 'Returning'
        when total_orders between 4 and 10 then 'Regular'
        when total_orders > 10 then 'VIP'
    end as customer_lifecycle,
    
    -- Value tier
    case
        when total_spent >= 5000000 then 'Platinum'
        when total_spent >= 2000000 then 'Gold'
        when total_spent >= 500000 then 'Silver'
        else 'Bronze'
    end as customer_value_tier,
    
    -- Return rate
    case 
        when total_orders > 0 
        then round(returned_orders * 100.0 / total_orders, 2)
        else 0 
    end as return_rate_pct,
    
    -- Metadata
    current_timestamp as _gold_loaded_at
    
from with_rfm
order by total_spent desc
