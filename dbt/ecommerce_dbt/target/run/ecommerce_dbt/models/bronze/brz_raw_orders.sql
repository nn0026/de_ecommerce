
  create view "ecommerce_db"."analytics_bronze"."brz_raw_orders__dbt_tmp"
    
    
  as (
    

select
    -- All columns from source as-is
    *,
    
    -- Bronze layer metadata
    'shopee_seller_center' as _source_system,
    current_timestamp as _bronze_loaded_at

from "ecommerce_db"."raw"."raw_orders"
  );