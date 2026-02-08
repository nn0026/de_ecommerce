select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select buyer_username
from "ecommerce_db"."analytics_gold"."agg_customer_summary"
where buyer_username is null



      
    ) dbt_internal_test