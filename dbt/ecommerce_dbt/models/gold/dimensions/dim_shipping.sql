-- GOLD LAYER: Shipping Dimension
-- Extracted from orders, enriched with carrier grouping

{{ config(
    materialized='table',
    schema='gold'
) }}

with source as (
    select * from {{ ref('slv_orders') }}
),

-- Extract unique shipping carriers
unique_carriers as (
    select distinct on (shipping_carrier)
        shipping_carrier,
        shipping_method
    from source
    where shipping_carrier is not null
    order by shipping_carrier, shipping_method
),

enriched as (
    select
        row_number() over (order by shipping_carrier) as shipping_id,
        trim(shipping_carrier) as shipping_carrier,
        trim(shipping_method) as shipping_method,
        
        -- Carrier grouping/normalization
        case
            when shipping_carrier ilike '%giao hang nhanh%' or shipping_carrier ilike '%ghn%' then 'GHN'
            when shipping_carrier ilike '%giao hang tiet kiem%' or shipping_carrier ilike '%ghtk%' then 'GHTK'
            when shipping_carrier ilike '%j&t%' or shipping_carrier ilike '%jt%' then 'J&T Express'
            when shipping_carrier ilike '%shopee express%' or shipping_carrier ilike '%spx%' then 'Shopee Express'
            when shipping_carrier ilike '%viettel%' then 'Viettel Post'
            when shipping_carrier ilike '%grab%' then 'GrabExpress'
            when shipping_carrier ilike '%ninja van%' then 'Ninja Van'
            when shipping_carrier ilike '%best%' then 'BEST Express'
            else 'Other'
        end as carrier_group,
        
        -- Carrier type
        case
            when shipping_carrier ilike '%shopee%' or shipping_carrier ilike '%spx%' then 'Platform Logistics'
            when shipping_carrier ilike '%grab%' then 'On-Demand'
            else 'Third Party Logistics'
        end as carrier_type,
        
        -- Shipping key
        md5(coalesce(shipping_carrier, 'unknown')) as shipping_key,
        
        -- Metadata
        current_timestamp as _gold_loaded_at
        
    from unique_carriers
)

select * from enriched
