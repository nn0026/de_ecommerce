-- SILVER LAYER: Customers
-- Extracted from raw_orders, cleaned and enriched with region classification



with source as (
    select * from "ecommerce_db"."analytics_bronze"."brz_raw_orders"
),

-- Extract unique customers from orders
unique_customers as (
    select distinct on (buyer_username)
        buyer_username,
        recipient_name,
        phone_number,
        province,
        district,
        ward,
        shipping_address,
        country,
        _source_system
    from source
    where buyer_username is not null and trim(buyer_username) != ''
    order by buyer_username, recipient_name
),

cleaned as (
    select
        -- Generate customer_id
        row_number() over (order by buyer_username) as customer_id,
        
        COALESCE(NULLIF(trim(buyer_username), ''), 'guest_' || row_number() over (order by recipient_name)) as buyer_username,
        trim(recipient_name) as recipient_name,
        trim(phone_number) as phone_number,
        
        -- Geography - cleaned
        trim(province) as province,
        trim(district) as district,
        trim(ward) as ward,
        trim(shipping_address) as shipping_address,
        coalesce(trim(country), 'VN') as country,
        
        -- Region classification (Vietnamese geography)
        case
            when province in ('Hồ Chí Minh', 'TP. Hồ Chí Minh', 'Ho Chi Minh', 'HCM', 'TP.HCM', 'Tp. Hồ Chí Minh') then 'South'
            when province in ('Hà Nội', 'Ha Noi', 'Hanoi', 'TP. Hà Nội') then 'North'
            when province in ('Đà Nẵng', 'Da Nang', 'TP. Đà Nẵng') then 'Central'
            when province in ('Cần Thơ', 'Can Tho', 'An Giang', 'Đồng Tháp', 'Bến Tre', 'Vĩnh Long', 'Tiền Giang', 'Long An', 'Kiên Giang', 'Hậu Giang', 'Sóc Trăng', 'Bạc Liêu', 'Cà Mau', 'Trà Vinh') then 'Mekong Delta'
            when province in ('Bình Dương', 'Đồng Nai', 'Bà Rịa - Vũng Tàu', 'Tây Ninh', 'Bình Phước') then 'Southeast'
            when province in ('Lâm Đồng', 'Đắk Lắk', 'Đắk Nông', 'Gia Lai', 'Kon Tum') then 'Central Highlands'
            when province in ('Thừa Thiên Huế', 'Quảng Nam', 'Quảng Ngãi', 'Bình Định', 'Phú Yên', 'Khánh Hòa', 'Ninh Thuận', 'Bình Thuận') then 'South Central Coast'
            when province in ('Hải Phòng', 'Quảng Ninh', 'Thái Bình', 'Nam Định', 'Ninh Bình', 'Hà Nam', 'Hưng Yên', 'Hải Dương', 'Bắc Ninh', 'Vĩnh Phúc') then 'Red River Delta'
            else 'Other'
        end as region,
        
        -- Customer key for joining (deterministic hash)
        md5(coalesce(buyer_username, '') || '|' || coalesce(phone_number, '')) as customer_key,
        
        -- Source metadata
        _source_system,
        
        -- Silver layer metadata
        current_timestamp as _silver_loaded_at
        
    from unique_customers
)

select * from cleaned