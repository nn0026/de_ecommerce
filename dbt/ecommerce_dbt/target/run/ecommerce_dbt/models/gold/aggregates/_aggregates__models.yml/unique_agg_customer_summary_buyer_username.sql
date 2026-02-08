select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    buyer_username as unique_field,
    count(*) as n_records

from "ecommerce_db"."analytics_gold"."agg_customer_summary"
where buyer_username is not null
group by buyer_username
having count(*) > 1



      
    ) dbt_internal_test