#!/usr/bin/env python3
"""
clean_transform_pipeline.py

Cleans and transforms ecommerce CSVs:
 - customers.csv
 - products.csv
 - orders.csv

Outputs:
 - clean_customers.csv
 - clean_products.csv
 - clean_orders.csv

Prints basic analytics to console.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
import phonenumbers
import re
from decimal import Decimal, ROUND_HALF_UP
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# ---------- CONFIG ----------
CUSTOMERS_CSV = "customers.csv"
PRODUCTS_CSV = "products.csv"
ORDERS_CSV = "orders.csv"

OUT_CUSTOMERS = "clean_customers.csv"
OUT_PRODUCTS = "clean_products.csv"
OUT_ORDERS = "clean_orders.csv"


# ---------- UTILITY FUNCTIONS ----------

def safe_read_csv(path):
    """Safely read CSV with common defaults."""
    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_values=["", "NA", "NaN"]
    )

def normalize_email(email):
    if not email or str(email).strip() == "":
        return np.nan
    e = str(email).lower()
    e = re.sub(r"\s+", "", e)
    return e

def normalize_phone(phone, default_region="IN"):
    if not phone or str(phone).strip() == "":
        return np.nan

    cleaned = re.sub(r"[^\d\+]", "", str(phone))

    try:
        parsed = phonenumbers.parse(cleaned, default_region)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except:
        pass

    # fallback: return digits if length reasonable
    digits = re.sub(r"\D", "", cleaned)
    return digits if len(digits) >= 8 else np.nan

def parse_date_safe(val):
    if not val or str(val).strip() == "":
        return pd.NaT

    try:
        return pd.to_datetime(val, utc=True)
    except:
        return pd.to_datetime(val, utc=True, errors="coerce")

def money_round(x, ndigits=2):
    try:
        d = Decimal(str(x)).quantize(
            Decimal(f"1.{'0'*ndigits}"),
            rounding=ROUND_HALF_UP
        )
        return float(d)
    except:
        return np.nan


# ---------- CLEAN CUSTOMERS ----------
def clean_customers(df):
    df = df.copy()

    df['customer_id'] = pd.to_numeric(df.get('customer_id'), errors='coerce').astype('Int64')
    df['name'] = df.get('name', "").astype(str).str.strip().replace({"": np.nan})
    df['email'] = df.get('email').apply(normalize_email)
    df['phone'] = df.get('phone').apply(normalize_phone)

    df['city'] = df.get('city', "").astype(str).str.title().replace({"": np.nan})
    df['state'] = df.get('state', "").astype(str).str.title().replace({"": np.nan})

    df['signup_date'] = df.get('signup_date').apply(parse_date_safe)

    before = len(df)
    df = df.dropna(subset=['customer_id', 'name'])
    print(f"customers: dropped {before - len(df)} invalid rows")

    df = df.sort_values(by='signup_date')

    df['dup_key'] = df['email'].fillna(
        df['phone'].fillna(
            df['name'] + df['signup_date'].astype(str)
        )
    )

    df = df.drop_duplicates(subset=['dup_key']).drop(columns=['dup_key'])
    return df.reset_index(drop=True)


# ---------- CLEAN PRODUCTS ----------
def clean_products(df):
    df = df.copy()

    df['product_id'] = pd.to_numeric(df.get('product_id'), errors='coerce').astype('Int64')
    df['product_name'] = df.get('product_name', "").astype(str).str.strip().replace({"": np.nan})
    df['category'] = df.get('category', "").astype(str).str.title().replace({"": np.nan})
    df['brand'] = df.get('brand', "").astype(str).str.title().replace({"": np.nan})

    df['price'] = pd.to_numeric(df.get('price'), errors='coerce').apply(money_round)
    df['stock_quantity'] = pd.to_numeric(df.get('stock_quantity'), errors='coerce').astype('Int64')

    before = len(df)
    df = df.dropna(subset=['product_id', 'product_name'])
    print(f"products: dropped {before - len(df)} invalid rows")

    df = df.drop_duplicates(subset=['product_id'])
    return df.reset_index(drop=True)


# ---------- CLEAN ORDERS ----------
def clean_orders(df, valid_customers, valid_products, price_map):
    df = df.copy()

    df['order_id'] = pd.to_numeric(df.get('order_id'), errors='coerce').astype('Int64')
    df['customer_id'] = pd.to_numeric(df.get('customer_id'), errors='coerce').astype('Int64')
    df['product_id'] = pd.to_numeric(df.get('product_id'), errors='coerce').astype('Int64')

    df['quantity'] = pd.to_numeric(df.get('quantity'), errors='coerce').fillna(1).astype(int)

    if 'order_timestamp' in df:
        df['order_timestamp'] = df['order_timestamp'].apply(parse_date_safe)
    else:
        df['order_timestamp'] = df.get('order_date', "").apply(parse_date_safe)

    df['shipping_city'] = df.get('shipping_city', "").astype(str).str.title().replace({"": np.nan})

    df['payment_method'] = df.get('payment_method', "Unknown").astype(str).str.title()
    df['status'] = df.get('status', "Unknown").astype(str).str.title()

    before = len(df)
    df = df[df['customer_id'].isin(valid_customers)]
    df = df[df['product_id'].isin(valid_products)]
    print(f"orders: dropped {before - len(df)} rows referencing invalid customer/product")

    def compute_value(row):
        pid = int(row['product_id'])
        price = price_map.get(pid)
        return money_round(price * row['quantity']) if price is not None else np.nan

    df['order_value'] = df.apply(compute_value, axis=1)

    df['order_timestamp'] = df['order_timestamp'].fillna(
        pd.to_datetime(datetime.now(timezone.utc), utc=True)
    )

    df = df.sort_values(by='order_timestamp')
    df = df.drop_duplicates(subset=['order_id'])

    return df.reset_index(drop=True)


# ---------- ANALYTICS ----------
def analytics(cust, prod, ords):
    print("\n=== ANALYTICS ===")

    print(f"Customers: {len(cust)}")
    print(f"Products:  {len(prod)}")
    print(f"Orders:    {len(ords)}")

    missing_price = prod['price'].isna().sum()
    print(f"Products missing price: {missing_price}")

    if ords['order_value'].notna().any():
        summary = (
            ords.groupby('product_id')
            .agg(units_sold=('quantity', 'sum'), revenue=('order_value', 'sum'))
            .reset_index()
            .sort_values('revenue', ascending=False)
        )

        summary = summary.merge(
            prod[['product_id', 'product_name']],
            on='product_id',
            how='left'
        )
        print("\nTop Revenue Products (Top 5):")
        print(summary.head(5).to_string(index=False))

    if ords['order_timestamp'].notna().any():
        ords['month'] = ords['order_timestamp'].dt.to_period('M')
        monthly = ords.groupby('month')['order_value'].sum().reset_index().sort_values('month')
        print("\nMonthly Revenue:")
        print(monthly.head(6).to_string(index=False))


# ---------- MAIN ----------
def main():
    print("Loading CSV files...")
    raw_cust = safe_read_csv(CUSTOMERS_CSV)
    raw_prod = safe_read_csv(PRODUCTS_CSV)
    raw_ord = safe_read_csv(ORDERS_CSV)

    print("Cleaning customers...")
    cust = clean_customers(raw_cust)

    print("Cleaning products...")
    prod = clean_products(raw_prod)

    print("Cleaning orders...")
    price_map = dict(zip(prod['product_id'], prod['price']))
    ords = clean_orders(
        raw_ord,
        valid_customers=set(cust['customer_id']),
        valid_products=set(prod['product_id']),
        price_map=price_map
    )

    print(f"Saving cleaned CSVs...")
    cust.to_csv(OUT_CUSTOMERS, index=False)
    prod.to_csv(OUT_PRODUCTS, index=False)

    analytics(cust, prod, ords)

    ords['order_timestamp'] = pd.to_datetime(ords['order_timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    ords.to_csv(OUT_ORDERS, index=False)

    print("\n✔ Pipeline completed successfully.")


if __name__ == "__main__":
    main()
