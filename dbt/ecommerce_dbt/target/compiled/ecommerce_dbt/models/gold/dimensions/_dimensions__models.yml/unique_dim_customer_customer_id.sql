
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from "ecommerce_db"."analytics_gold"."dim_customer"
where customer_id is not null
group by customer_id
having count(*) > 1


