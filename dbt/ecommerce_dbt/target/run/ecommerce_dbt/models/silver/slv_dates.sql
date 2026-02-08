
  
    

  create  table "ecommerce_db"."analytics_silver"."slv_dates__dbt_tmp"
  
  
    as
  
  (
    -- SILVER LAYER: Dates
-- Generated from raw_orders date range, enriched with Shopee sale events



with source as (
    select * from "ecommerce_db"."analytics_bronze"."brz_raw_orders"
),

-- Extract unique dates from orders
unique_dates as (
    select distinct
        cast(order_date as date) as date_value
    from source
    where order_date is not null
),

enriched as (
    select
        -- Date key
        cast(to_char(date_value, 'YYYYMMDD') as integer) as date_key,
        date_value as full_date,
        
        -- Date parts
        extract(year from date_value) as year,
        extract(quarter from date_value) as quarter,
        extract(month from date_value) as month,
        extract(week from date_value) as week_of_year,
        extract(day from date_value) as day_of_month,
        extract(dow from date_value) as day_of_week,
        
        -- Date names
        to_char(date_value, 'Month') as month_name,
        to_char(date_value, 'Day') as day_name,
        to_char(date_value, 'Mon') as month_abbr,
        to_char(date_value, 'Dy') as day_abbr,
        
        -- Flags
        case when extract(dow from date_value) in (0, 6) then true else false end as is_weekend,
        case when extract(dow from date_value) between 1 and 5 then true else false end as is_weekday,
        
        -- Shopee Double-Day Sale Events (major e-commerce events in SEA)
        case 
            when extract(month from date_value) = extract(day from date_value) 
                 and extract(day from date_value) <= 12 then true
            else false
        end as is_double_day_sale,
        
        -- Sale event names
        case 
            when extract(month from date_value) = 1 and extract(day from date_value) = 1 then '1.1 New Year Sale'
            when extract(month from date_value) = 2 and extract(day from date_value) = 2 then '2.2 Sale'
            when extract(month from date_value) = 3 and extract(day from date_value) = 3 then '3.3 Sale'
            when extract(month from date_value) = 4 and extract(day from date_value) = 4 then '4.4 Sale'
            when extract(month from date_value) = 5 and extract(day from date_value) = 5 then '5.5 Sale'
            when extract(month from date_value) = 6 and extract(day from date_value) = 6 then '6.6 Mid-Year Sale'
            when extract(month from date_value) = 7 and extract(day from date_value) = 7 then '7.7 Sale'
            when extract(month from date_value) = 8 and extract(day from date_value) = 8 then '8.8 Sale'
            when extract(month from date_value) = 9 and extract(day from date_value) = 9 then '9.9 Super Shopping Day'
            when extract(month from date_value) = 10 and extract(day from date_value) = 10 then '10.10 Sale'
            when extract(month from date_value) = 11 and extract(day from date_value) = 11 then '11.11 Singles Day'
            when extract(month from date_value) = 12 and extract(day from date_value) = 12 then '12.12 Birthday Sale'
            else null
        end as sale_event_name,
        
        -- Vietnamese holidays
        case
            when extract(month from date_value) = 1 and extract(day from date_value) = 1 then true  -- New Year
            when extract(month from date_value) = 4 and extract(day from date_value) = 30 then true  -- Liberation Day
            when extract(month from date_value) = 5 and extract(day from date_value) = 1 then true  -- Labour Day
            when extract(month from date_value) = 9 and extract(day from date_value) = 2 then true  -- Independence Day
            else false
        end as is_vn_holiday,
        
        -- Period helpers
        date_trunc('month', date_value)::date as first_day_of_month,
        (date_trunc('month', date_value) + interval '1 month' - interval '1 day')::date as last_day_of_month,
        date_trunc('week', date_value)::date as first_day_of_week,
        
        -- Source metadata
        'shopee_seller_center' as _source_system,
        
        -- Silver layer metadata
        current_timestamp as _silver_loaded_at
        
    from unique_dates
)

select * from enriched
  );
  