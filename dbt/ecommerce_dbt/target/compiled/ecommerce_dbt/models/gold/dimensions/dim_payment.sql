-- GOLD LAYER: Payment Dimension
-- Extracted from orders, enriched with payment grouping



with source as (
    select * from "ecommerce_db"."analytics_silver"."slv_orders"
),

-- Extract unique payment methods
unique_payments as (
    select distinct
        payment_method
    from source
    where payment_method is not null
),

enriched as (
    select
        row_number() over (order by payment_method) as payment_method_id,
        trim(payment_method) as payment_method,
        
        -- Payment method grouping
        case
            when payment_method ilike '%cod%' or payment_method ilike '%thanh toán khi nhận%' then 'COD'
            when payment_method ilike '%shopee%' or payment_method ilike '%spay%' or payment_method ilike '%shopeepay%' then 'ShopeePay'
            when payment_method ilike '%momo%' then 'MoMo'
            when payment_method ilike '%zalo%' then 'ZaloPay'
            when payment_method ilike '%visa%' or payment_method ilike '%mastercard%' or payment_method ilike '%credit%' or payment_method ilike '%thẻ%' then 'Credit/Debit Card'
            when payment_method ilike '%bank%' or payment_method ilike '%ngân hàng%' then 'Bank Transfer'
            when payment_method ilike '%vnpay%' then 'VNPay'
            else 'Other'
        end as payment_group,
        
        -- Payment type
        case
            when payment_method ilike '%cod%' or payment_method ilike '%thanh toán khi nhận%' then 'Cash'
            when payment_method ilike '%shopee%' or payment_method ilike '%momo%' or payment_method ilike '%zalo%' or payment_method ilike '%vnpay%' then 'E-Wallet'
            when payment_method ilike '%visa%' or payment_method ilike '%mastercard%' or payment_method ilike '%credit%' or payment_method ilike '%thẻ%' then 'Card'
            when payment_method ilike '%bank%' or payment_method ilike '%ngân hàng%' then 'Bank'
            else 'Other'
        end as payment_type,
        
        -- Is digital payment
        case
            when payment_method ilike '%cod%' or payment_method ilike '%thanh toán khi nhận%' then false
            else true
        end as is_digital_payment,
        
        -- Payment key
        md5(coalesce(payment_method, 'unknown')) as payment_key,
        
        -- Metadata
        current_timestamp as _gold_loaded_at
        
    from unique_payments
)

select * from enriched