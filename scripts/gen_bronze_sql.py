# -*- coding: utf-8 -*-
"""Generate Bronze SQL from actual PostgreSQL column names"""
import psycopg2
import unicodedata
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Column mapping: vietnamese -> english
COLUMN_MAP = {
    "Mã đơn hàng": "order_id",
    "Mã Kiện Hàng": "package_id", 
    "Ngày đặt hàng": "order_date",
    "Trạng Thái Đơn Hàng": "order_status",
    "Loại đơn hàng": "order_type",
    "Trạng thái Trả hàng/Hoàn tiền": "return_status",
    "SKU sản phẩm": "product_sku",
    "Tên sản phẩm": "product_name",
    "SKU phân loại hàng": "variant_sku",
    "Tên phân loại hàng": "variant_name",
    "Cân nặng sản phẩm": "product_weight",
    "Tổng cân nặng": "total_weight",
    "Sản Phẩm Bán Chạy": "is_bestseller",
    "Giá gốc": "original_price",
    "Giá ưu đãi": "discount_price",
    "Số lượng": "quantity",
    "Số lượng sản phẩm được hoàn trả": "returned_quantity",
    "Tổng giá bán (sản phẩm)": "total_product_price",
    "Tổng giá trị đơn hàng (VND)": "order_total_vnd",
    "Người bán trợ giá": "seller_discount",
    "Được Shopee trợ giá": "shopee_discount",
    "Tổng số tiền được người bán trợ giá": "total_seller_discount",
    "Mã giảm giá của Shop": "shop_voucher",
    "Mã giảm giá của Shopee": "shopee_voucher",
    "Hoàn Xu": "coins_cashback",
    "Shopee Xu được hoàn": "shopee_coins_returned",
    "Chỉ tiêu Combo Khuyến Mãi": "combo_promo_target",
    "Giảm giá từ combo Shopee": "shopee_combo_discount",
    "Giảm giá từ Combo của Shop": "shop_combo_discount",
    "Số tiền được giảm khi thanh toán bằng thẻ Ghi nợ": "debit_card_discount",
    "Trade-in Discount": "tradein_discount",
    "Trade-in Bonus": "tradein_bonus",
    "Trade-in Bonus by Seller": "tradein_bonus_seller",
    "Mã vận đơn": "tracking_number",
    "Đơn Vị Vận Chuyển": "shipping_carrier",
    "Phương thức giao hàng": "shipping_method",
    "Ngày giao hàng dự kiến": "expected_delivery_date",
    "Ngày gửi hàng": "ship_date",
    "Thời gian giao hàng": "actual_delivery_date",
    "Phí vận chuyển (dự kiến)": "shipping_fee_estimated",
    "Phí vận chuyển mà người mua trả": "shipping_fee_paid",
    "Phí vận chuyển tài trợ bởi Shopee (dự kiến)": "shipping_subsidy",
    "Phí trả hàng": "return_fee",
    "Người Mua": "buyer_username",
    "Tên Người nhận": "recipient_name",
    "Số điện thoại": "phone_number",
    "Tỉnh/Thành phố": "province",
    "TP / Quận / Huyện": "district",
    "Quận": "ward",
    "Địa chỉ nhận hàng": "shipping_address",
    "Quốc gia": "country",
    "Tổng số tiền người mua thanh toán": "total_paid",
    "Thời gian hoàn thành đơn hàng": "order_completed_date",
    "Thời gian đơn hàng được thanh toán": "payment_date",
    "Phương thức thanh toán": "payment_method",
    "Phí cố định": "fixed_fee",
    "Phí Dịch Vụ": "service_fee",
    "Phí thanh toán": "payment_fee",
    "Tiền ký quỹ": "deposit",
    "Nhận xét từ Người mua": "buyer_review",
    "Ghi chú": "note",
    "source_file": "source_file",
}

def normalize(s):
    """Normalize unicode to NFC form"""
    return unicodedata.normalize('NFC', s)

def main():
    # Connect to PostgreSQL (use postgres host inside Docker, localhost outside)
    import os
    host = os.environ.get('POSTGRES_HOST', 'localhost')
    port = int(os.environ.get('POSTGRES_PORT', '5434'))
    if host == 'postgres':
        port = 5432
    conn = psycopg2.connect(
        host=host,
        port=port,
        database="ecommerce_db",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    
    # Get actual column names from database
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema='raw' AND table_name='raw_orders' 
        ORDER BY ordinal_position
    """)
    db_columns = [row[0] for row in cur.fetchall()]
    
    # Normalize the mapping keys
    normalized_map = {normalize(k): v for k, v in COLUMN_MAP.items()}
    
    # Generate SELECT statements
    select_lines = []
    for col in db_columns:
        normalized_col = normalize(col)
        if normalized_col in normalized_map:
            eng_name = normalized_map[normalized_col]
            select_lines.append(f'    "{col}" as {eng_name}')
        else:
            # Try to find partial match
            matched = False
            for vn, eng in normalized_map.items():
                if vn in normalized_col or normalized_col in vn:
                    select_lines.append(f'    "{col}" as {eng}')
                    matched = True
                    break
            if not matched:
                # Keep original with safe name
                safe_name = col.lower().replace(' ', '_').replace('/', '_')
                select_lines.append(f'    "{col}" as "{safe_name}"')
                print(f"UNMATCHED: {col}")
    
    # Print SQL
    print("{{ config(")
    print("    materialized='view',")
    print("    schema='bronze'")
    print(") }}")
    print()
    print("select")
    print(",\n".join(select_lines) + ",")
    print("    'shopee_seller_center' as _source_system,")
    print("    current_timestamp as _bronze_loaded_at")
    print()
    print("from {{ source('raw', 'raw_orders') }}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
