select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    shipping_id as unique_field,
    count(*) as n_records

from "ecommerce_db"."analytics_gold"."dim_shipping"
where shipping_id is not null
group by shipping_id
having count(*) > 1



      
    ) dbt_internal_test