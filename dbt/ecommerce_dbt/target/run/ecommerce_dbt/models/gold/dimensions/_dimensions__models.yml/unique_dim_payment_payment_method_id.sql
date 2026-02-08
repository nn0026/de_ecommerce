select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    payment_method_id as unique_field,
    count(*) as n_records

from "ecommerce_db"."analytics_gold"."dim_payment"
where payment_method_id is not null
group by payment_method_id
having count(*) > 1



      
    ) dbt_internal_test