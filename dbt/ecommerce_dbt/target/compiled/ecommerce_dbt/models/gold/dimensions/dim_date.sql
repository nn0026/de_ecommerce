-- GOLD LAYER: Date Dimension
-- Business-ready calendar with Shopee events



select
    date_key,
    full_date as date,
    
    -- Date parts
    year,
    quarter,
    month,
    week_of_year,
    day_of_month,
    day_of_week,
    
    -- Date names
    month_name,
    day_name,
    month_abbr,
    day_abbr,
    
    -- Year-Month key for reporting
    year * 100 + month as year_month_key,
    to_char(full_date, 'YYYY-MM') as year_month,
    
    -- Flags
    is_weekend,
    is_weekday,
    is_double_day_sale,
    is_vn_holiday,
    
    -- Sale events
    sale_event_name,
    
    -- Period helpers
    first_day_of_month,
    last_day_of_month,
    first_day_of_week,
    
    -- Relative date flags (useful for dashboards)
    case when full_date = current_date then true else false end as is_today,
    case when full_date = current_date - interval '1 day' then true else false end as is_yesterday,
    case when full_date >= date_trunc('month', current_date) then true else false end as is_current_month,
    case when full_date >= date_trunc('year', current_date) then true else false end as is_current_year,
    
    -- Metadata
    current_timestamp as _gold_loaded_at
    
from "ecommerce_db"."analytics_silver"."slv_dates"