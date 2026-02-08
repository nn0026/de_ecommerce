# -*- coding: utf-8 -*-
import pandas as pd
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

df = pd.read_csv('raw_data/orders_merged.csv', encoding='utf-8-sig')

print("="*60)
print("DATA QUALITY CHECK - orders_merged.csv (547 rows)")
print("="*60)

print(f"\nShape: {df.shape}")
print(f"Columns: {len(df.columns)}")

# Key columns to check
cols_to_check = {
    'order_type': 'Loại đơn hàng',
    'return_status': 'Trạng thái Trả hàng/Hoàn tiền',
    'product_name': 'Tên sản phẩm',
    'buyer_username': 'Người Mua',
    'order_id': 'Mã đơn hàng',
    'order_total': 'Tổng giá trị đơn hàng (VND)',
    'quantity': 'Số lượng',
    'total_product_price': 'Tổng giá bán (sản phẩm)',
}

print("\n=== KEY COLUMNS ===")
for eng, vn in cols_to_check.items():
    if vn in df.columns:
        non_null = df[vn].notna().sum()
        non_empty = (df[vn].astype(str).str.strip() != '').sum()
        unique = df[vn].nunique()
        print(f"{eng}: {non_null}/{len(df)} non-null, {non_empty} non-empty, {unique} unique")

print("\n=== GMV CALCULATION ===")
# Unique orders
unique_orders = df['Mã đơn hàng'].nunique()
print(f"Unique Orders: {unique_orders}")

# Total GMV (sum order_total once per order)
order_totals = df.groupby('Mã đơn hàng')['Tổng giá trị đơn hàng (VND)'].first()
total_gmv = order_totals.sum()
print(f"Total GMV: {total_gmv:,.0f} VND ({total_gmv/1_000_000:.2f} million)")

# AOV
aov = total_gmv / unique_orders
print(f"AOV: {aov:,.0f} VND")

# Total rows (line items)
print(f"Total Line Items: {len(df)}")

print("\n=== SAMPLE VALUES ===")
for eng, vn in [('order_type', 'Loại đơn hàng'), ('return_status', 'Trạng thái Trả hàng/Hoàn tiền')]:
    if vn in df.columns:
        print(f"\n{eng} values:")
        print(df[vn].value_counts(dropna=False).head(10))

print("\n=== PRODUCT CATEGORIES (from product_name) ===")
if 'Tên sản phẩm' in df.columns:
    print(df['Tên sản phẩm'].value_counts().head(10))
