
    
    

select
    buyer_username as unique_field,
    count(*) as n_records

from "ecommerce_db"."analytics_gold"."agg_customer_summary"
where buyer_username is not null
group by buyer_username
having count(*) > 1


