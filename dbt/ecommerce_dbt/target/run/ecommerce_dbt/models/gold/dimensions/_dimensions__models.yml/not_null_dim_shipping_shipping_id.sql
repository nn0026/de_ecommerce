select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select shipping_id
from "ecommerce_db"."analytics_gold"."dim_shipping"
where shipping_id is null



      
    ) dbt_internal_test