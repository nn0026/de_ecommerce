
  
    

  create  table "ecommerce_db"."analytics_gold"."fct_orders__dbt_tmp"
  
  
    as
  
  (
    -- GOLD LAYER: Fact Orders
-- Main fact table - Star Schema center
-- Joins all dimensions with measures



with orders as (
    select * from "ecommerce_db"."analytics_silver"."slv_orders"
),

customers as (
    select customer_id, buyer_username, customer_key, region, customer_segment
    from "ecommerce_db"."analytics_gold"."dim_customer"
),

products as (
    select distinct on (product_name)
        product_id, product_name, product_key, main_category, product_tier
    from "ecommerce_db"."analytics_gold"."dim_product"
    order by product_name, product_id
),

dates as (
    select date_key, date, is_double_day_sale, sale_event_name
    from "ecommerce_db"."analytics_gold"."dim_date"
),

shipping as (
    select shipping_id, shipping_carrier, shipping_key, carrier_group
    from "ecommerce_db"."analytics_gold"."dim_shipping"
),

payment as (
    select payment_method_id, payment_method, payment_key, payment_group
    from "ecommerce_db"."analytics_gold"."dim_payment"
),

-- Customer order sequence (distinct orders only)
order_sequence as (
    select distinct on (order_id)
        order_id,
        buyer_username,
        order_date
    from orders
    order by order_id, order_date
),

order_seq_numbered as (
    select
        order_id,
        buyer_username,
        row_number() over (
            partition by buyer_username 
            order by order_date
        ) as customer_order_seq
    from order_sequence
),

fact_orders as (
    select
        -- Primary key
        o.order_id,
        
        -- Dimension keys (foreign keys)
        c.customer_id,
        p.product_id,
        d.date_key as order_date_key,
        s.shipping_id,
        pm.payment_method_id,
        
        -- Surrogate keys (for BI tools)
        c.customer_key,
        p.product_key,
        s.shipping_key,
        pm.payment_key,
        
        -- Natural keys (for reference)
        o.package_id,
        o.tracking_number,
        o.buyer_username,
        
        -- Date dimensions
        o.order_date,
        o.expected_delivery_date,
        o.actual_delivery_date,
        o.order_completed_date,
        o.payment_date,
        
        -- Delivery metrics
        case
            when o.actual_delivery_date is not null and o.order_date is not null
            then extract(day from o.actual_delivery_date - o.order_date)
            else null
        end as delivery_days,
        
        case
            when o.actual_delivery_date is not null and o.expected_delivery_date is not null
            then case 
                when o.actual_delivery_date <= o.expected_delivery_date then 'On Time'
                else 'Late'
            end
            else 'Unknown'
        end as delivery_status,
        
        -- Order status
        o.order_status,
        o.order_type,
        o.return_status,
        
        -- Product details (denormalized for performance)
        o.product_name,
        o.variant_name,
        o.product_weight,
        p.main_category,
        
        -- Quantity and pricing (MEASURES)
        o.quantity,
        o.original_price,
        o.discount_price,
        o.total_product_price,
        o.order_total_vnd,
        
        -- Discount breakdown (MEASURES)
        o.seller_discount,
        o.shopee_discount,
        o.shop_voucher,
        o.shopee_voucher,
        o.coins_cashback,
        o.total_discount,
        
        -- Shipping costs (MEASURES)
        o.shipping_fee_estimated,
        o.shipping_fee_paid,
        o.shipping_subsidy,
        
        -- Payment (MEASURES)
        o.total_paid,
        o.payment_method,
        
        -- Fees (MEASURES)
        o.fixed_fee,
        o.service_fee,
        o.payment_fee,
        o.deposit,
        
        -- Calculated measures
        o.total_paid - o.fixed_fee - o.service_fee - o.payment_fee as net_revenue,
        
        case
            when o.original_price > 0 
            then round(((o.total_paid - o.fixed_fee - o.service_fee - o.payment_fee) / (o.original_price * o.quantity)) * 100, 2)
            else 0
        end as profit_margin_pct,
        
        -- Order value tier
        case
            when o.order_total_vnd >= 1000000 then 'Premium (1M+)'
            when o.order_total_vnd >= 500000 then 'High (500K-1M)'
            when o.order_total_vnd >= 200000 then 'Medium (200K-500K)'
            when o.order_total_vnd >= 100000 then 'Low (100K-200K)'
            else 'Micro (<100K)'
        end as order_value_tier,
        
        -- Customer location (denormalized)
        o.province,
        o.district,
        c.region,
        
        -- Shipping info (denormalized)
        o.shipping_carrier,
        s.carrier_group,
        
        -- Payment info (denormalized)
        pm.payment_group,
        
        -- Customer order sequence
        seq.customer_order_seq,
        case when seq.customer_order_seq = 1 then 'New' else 'Repeat' end as new_vs_repeat,
        
        -- Sale event (denormalized from date)
        d.is_double_day_sale,
        d.sale_event_name,
        
        -- Flags
        o.is_bestseller,
        case when o.return_status is not null and o.return_status != '' then true else false end as is_returned,
        case when o.order_status in ('Hoàn thành', 'complete', 'completed', 'Completed') then true else false end as is_completed,
        case when o.order_status in ('Đã hủy', 'cancelled', 'Cancelled', 'cancel') then true else false end as is_cancelled,
        
        -- Metadata
        o.source_file,
        o.source_loaded_at,
        current_timestamp as _gold_loaded_at
        
    from orders o
    left join customers c on o.buyer_username = c.buyer_username
    left join products p on o.product_name = p.product_name
    left join dates d on cast(o.order_date as date) = d.date
    left join shipping s on o.shipping_carrier = s.shipping_carrier
    left join payment pm on o.payment_method = pm.payment_method
    left join order_seq_numbered seq on o.order_id = seq.order_id
)

select * from fact_orders
  );
  