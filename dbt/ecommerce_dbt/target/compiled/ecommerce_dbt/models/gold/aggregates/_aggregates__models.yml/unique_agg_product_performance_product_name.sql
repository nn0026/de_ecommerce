
    
    

select
    product_name as unique_field,
    count(*) as n_records

from "ecommerce_db"."analytics_gold"."agg_product_performance"
where product_name is not null
group by product_name
having count(*) > 1


