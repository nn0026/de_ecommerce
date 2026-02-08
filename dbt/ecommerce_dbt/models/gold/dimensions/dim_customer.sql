-- GOLD LAYER: Customer Dimension
-- Business-ready customer data with RFM segmentation

{{ config(
    materialized='table',
    schema='gold'
) }}

with customers as (
    select * from {{ ref('slv_customers') }}
),

-- Calculate order aggregates per customer
-- FIX: First aggregate to ORDER grain to avoid duplicating order_total_vnd per product line
order_stats as (
    with customer_orders as (
        select
            buyer_username,
            order_id,
            max(order_total_vnd) as order_total_vnd,
            max(order_date) as order_date
        from {{ ref('slv_orders') }}
        group by buyer_username, order_id
    )
    select
        buyer_username,
        count(distinct order_id) as total_orders,
        sum(order_total_vnd) as total_spent,
        avg(order_total_vnd) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        current_date - max(order_date)::date as days_since_last_order
    from customer_orders
    group by buyer_username
),

-- RFM calculation
rfm_calc as (
    select
        c.*,
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.total_spent, 0) as total_spent,
        coalesce(o.avg_order_value, 0) as avg_order_value,
        o.first_order_date,
        o.last_order_date,
        coalesce(o.days_since_last_order, 999) as days_since_last_order,
        
        -- RFM Scores (1-5 scale)
        ntile(5) over (order by coalesce(o.days_since_last_order, 999) desc) as r_score,
        ntile(5) over (order by coalesce(o.total_orders, 0)) as f_score,
        ntile(5) over (order by coalesce(o.total_spent, 0)) as m_score
        
    from customers c
    left join order_stats o on c.buyer_username = o.buyer_username
),

final as (
    select
        customer_id,
        raw_buyer_username,
        buyer_username,
        is_guest_customer,
        recipient_name,
        phone_number,
        
        -- Geography
        province,
        district,
        ward,
        shipping_address,
        country,
        region,
        
        -- Customer key
        customer_key,
        
        -- Order metrics
        total_orders,
        total_spent,
        avg_order_value,
        first_order_date,
        last_order_date,
        days_since_last_order,
        
        -- RFM Scores
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
            else 'Prospect'
        end as customer_lifecycle,
        
        -- Customer value tier
        case
            when total_spent >= 5000000 then 'Platinum'
            when total_spent >= 2000000 then 'Gold'
            when total_spent >= 500000 then 'Silver'
            else 'Bronze'
        end as customer_value_tier,
        
        -- Metadata
        current_timestamp as _gold_loaded_at
        
    from rfm_calc
)

select * from final
