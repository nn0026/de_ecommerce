select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select product_name
from "ecommerce_db"."analytics_gold"."agg_product_performance"
where product_name is null



      
    ) dbt_internal_test