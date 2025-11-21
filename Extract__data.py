#!/usr/bin/env python3
"""
E-commerce Synthetic Data Generator
----------------------------------
Creates realistic, reproducible synthetic datasets for customers, products and orders.

Features:
- Deterministic by default (seeded) for reproducible tests.
- Ensures exact `num_products` and valid references in orders.
- Uses Faker('en_IN') for Indian realistic values.
- Supports three operation modes:
    * batch  - generate CSV snapshots (customers.csv, products.csv, orders.csv)
    * stream-file - append timestamped events to a streaming CSV (orders_stream.csv)
    * kafka  - (optional) publish events to Kafka topic (if kafka-python installed)

Usage examples:
  python ecommerce_data_pipeline.py --mode batch --num_customers 100 --num_products 50 --num_orders 200
  python ecommerce_data_pipeline.py --mode stream-file --frequency 2
  python ecommerce_data_pipeline.py --mode kafka --kafka-bootstrap localhost:9092 --kafka-topic orders-topic

Requirements:
  pip install faker
  (optional) pip install kafka-python

"""

import argparse
import csv
import json
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

try:
    from faker import Faker
except Exception as e:
    raise SystemExit("Faker is required. Install with `pip install faker`\n" + str(e))

# Optional Kafka dependency - used only if user chooses kafka mode
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except Exception:
    KAFKA_AVAILABLE = False

# ---------------------------
# Configuration & defaults
# ---------------------------
DEFAULT_SEED = 42
DEFAULT_LOCALE = 'en_IN'
BATCH_CUSTOMERS_FILE = 'customers.csv'
BATCH_PRODUCTS_FILE = 'products.csv'
BATCH_ORDERS_FILE = 'orders.csv'
STREAM_ORDERS_FILE = 'orders_stream.csv'

# ---------------------------
# Initialize Faker and seeding
# ---------------------------
fake = Faker(DEFAULT_LOCALE)

# ---------------------------
# Data generation functions
# ---------------------------

def generate_customers(num_customers=100, seed=None):
    """Generate a list of customer dicts with unique emails and phones."""
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)
    fake_unique = Faker(DEFAULT_LOCALE).unique

    customers = []
    for i in range(1, num_customers + 1):
        # Some Faker phone formats might include punctuation; keep as string
        customers.append({
            'customer_id': i,
            'name': fake_unique.name(),
            'email': fake_unique.email(),
            'phone': fake_unique.phone_number(),
            'city': fake.city(),
            'state': fake.state(),
            'signup_date': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
        })
    return customers


def generate_products(num_products=50, seed=None):
    """Generate exactly num_products products by flattening a catalog and sampling.

    Ensures product_id values go from 1..num_products, and returns a list of product dicts.
    """
    if seed is not None:
        random.seed(seed)

    product_catalog = {
        'Electronics': [
            'Samsung Galaxy S23', 'iPhone 14 Pro', 'OnePlus 11', 'Sony WH-1000XM5 Headphones',
            'Dell XPS 15 Laptop', 'iPad Air', 'Apple Watch Series 8', 'Canon EOS R6',
            'Sony PlayStation 5', 'Nintendo Switch', 'Kindle Paperwhite', 'GoPro Hero 11'
        ],
        'Clothing': [
            "Levi's Jeans", 'Nike Air Max Shoes', 'Adidas T-Shirt', 'Puma Hoodie',
            'Raymond Formal Shirt', 'Allen Solly Trousers', 'Zara Dress', 'H&M Jacket',
            'Bata Sandals', 'Woodland Boots', 'US Polo Assn Shirt', 'Levis Denim Jacket'
        ],
        'Books': [
            'Atomic Habits by James Clear', 'The Psychology of Money', 'Sapiens by Yuval Harari',
            'Ikigai: Japanese Secret', 'Rich Dad Poor Dad', 'The Alchemist', 'Think and Grow Rich',
            'Deep Work by Cal Newport', 'The Subtle Art', 'Zero to One by Peter Thiel'
        ],
        'Home & Kitchen': [
            'Philips Air Fryer', 'Prestige Pressure Cooker', 'Havells Mixer Grinder',
            'Milton Water Bottle', 'Cello Storage Containers', 'Pigeon Gas Stove',
            'Bajaj Room Heater', 'Usha Fan', 'Godrej Almirah', 'Nilkamal Chair'
        ],
        'Sports & Fitness': [
            'Nivia Football', 'Yonex Badminton Racket', 'Cosco Cricket Bat', 'Adidas Gym Bag',
            'Decathlon Yoga Mat', 'Protoner Dumbbells', 'Strauss Resistance Bands',
            'Nivia Running Shoes', 'Fitbit Charge 5', 'Boldfit Gym Gloves'
        ]
    }

    # Flatten catalog to (category, product_name) tuples
    flat = []
    for cat, items in product_catalog.items():
        for it in items:
            flat.append((cat, it))

    # If the requested number of products exceeds unique catalog items, allow sampling with replacement
    if num_products > len(flat):
        chosen = [random.choice(flat) for _ in range(num_products)]
    else:
        chosen = random.sample(flat, num_products)

    products = []
    pid = 1
    for cat, name in chosen:
        # Extract brand naively from first token but keep original name intact
        brand = name.split()[0]
        products.append({
            'product_id': pid,
            'product_name': name,
            'category': cat,
            'brand': brand,
            'price': round(random.uniform(299, 89999), 2),
            'stock_quantity': random.randint(5, 500)
        })
        pid += 1

    return products


def generate_orders(num_orders=200, customers=None, products=None, seed=None, include_order_value=True):
    """Generate orders using actual customer and product lists to ensure referential integrity.

    Each order contains an ISO8601 timestamp in UTC (order_timestamp) for streaming realism.
    """
    if customers is None or products is None:
        raise ValueError('customers and products lists must be provided to generate_orders')

    if seed is not None:
        random.seed(seed)

    payment_methods = ['Credit Card', 'Debit Card', 'UPI', 'Net Banking', 'Cash on Delivery', 'Wallet']
    statuses = ['Completed', 'Pending', 'Cancelled', 'Shipped', 'Delivered']

    max_customer_id = max(c['customer_id'] for c in customers)
    product_map = {p['product_id']: p for p in products}

    orders = []
    for i in range(1, num_orders + 1):
        pid = random.choice(list(product_map.keys()))
        qty = random.randint(1, 5)
        price = product_map[pid]['price']
        order_value = round(price * qty, 2)

        orders.append({
            'order_id': i,
            'customer_id': random.randint(1, max_customer_id),
            'product_id': pid,
            'quantity': qty,
            'order_timestamp': datetime.utcnow().isoformat() + 'Z',
            'payment_method': random.choice(payment_methods),
            'status': random.choices(statuses, weights=[50, 15, 10, 15, 10])[0],
            'shipping_city': fake.city(),
            **({'order_value': order_value} if include_order_value else {})
        })
    return orders

# ---------------------------
# CSV helpers
# ---------------------------

def save_to_csv(data, filename, fieldnames, mode='w'):
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    write_header = (mode == 'w')
    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} records to {filename}")

# ---------------------------
# Streaming helpers
# ---------------------------

def stream_append_orders_loop(start_id=1, frequency_seconds=2, customers=None, products=None, seed=None, outfile=STREAM_ORDERS_FILE):
    """Append events to a CSV file at a given frequency (seconds).

    This simulates a simple streaming source. The CSV file will have an `order_id` that increments
    each event and an `order_timestamp` with ISO8601 UTC time.
    """
    if customers is None or products is None:
        raise ValueError('customers and products required for streaming')

    # Determine starting id if file exists
    if Path(outfile).exists():
        try:
            with open(outfile, 'r', newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
                if rows:
                    start_id = int(rows[-1]['order_id']) + 1
        except Exception:
            print('Could not read existing stream file - starting at provided start_id')

    fieldnames = ['order_id', 'customer_id', 'product_id', 'quantity', 'order_timestamp', 'payment_method', 'status', 'shipping_city', 'order_value']

    current_id = start_id
    print(f"Starting stream append to {outfile} at order_id={current_id} (every {frequency_seconds}s). Ctrl-C to stop.")
    try:
        while True:
            order = generate_orders(num_orders=1, customers=customers, products=products, seed=seed)[0]
            # override order_id to maintain global increment
            order['order_id'] = current_id
            # Append mode
            save_to_csv([order], outfile, fieldnames=fieldnames, mode='a')
            current_id += 1
            time.sleep(frequency_seconds)
    except KeyboardInterrupt:
        print('\nStreaming append interrupted by user, exiting.')


def kafka_produce_orders(bootstrap_servers, topic, customers, products, frequency_seconds=1, seed=None):
    """Produce JSON-encoded order events to Kafka topic. Requires kafka-python.

    This function will block and produce indefinitely until interrupted.
    """
    if not KAFKA_AVAILABLE:
        raise RuntimeError('kafka-python library is not available. Install with `pip install kafka-python`')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    order_counter = 1
    try:
        print(f"Producing to Kafka topic {topic} on {bootstrap_servers} every {frequency_seconds}s. Ctrl-C to stop.")
        while True:
            order = generate_orders(num_orders=1, customers=customers, products=products, seed=seed)[0]
            # use a UUID for event id to enable dedupe downstream
            event = {
                'event_id': str(uuid.uuid4()),
                'order': order
            }
            producer.send(topic, event)
            producer.flush()
            print(f"Produced event_id={event['event_id']} order_id={order['order_id']}")
            order_counter += 1
            time.sleep(frequency_seconds)
    except KeyboardInterrupt:
        print('\nKafka producer interrupted by user, exiting.')
    finally:
        producer.close()

# ---------------------------
# Main CLI
# ---------------------------

def parse_args():
    p = argparse.ArgumentParser(description='Synthetic e-commerce data generator')
    p.add_argument('--mode', choices=['batch', 'stream-file', 'kafka'], default='batch', help='Operation mode')

    # Batch options
    p.add_argument('--num_customers', type=int, default=100)
    p.add_argument('--num_products', type=int, default=50)
    p.add_argument('--num_orders', type=int, default=200)

    # Streaming options
    p.add_argument('--frequency', type=float, default=2.0, help='Seconds between events in streaming modes')

    # Seed for reproducibility; if omitted, non-deterministic
    p.add_argument('--seed', type=int, default=DEFAULT_SEED)
    p.add_argument('--no-seed', action='store_true', help='Do not seed RNGs (non-deterministic runs)')

    # Kafka options
    p.add_argument('--kafka-bootstrap', default='localhost:9092')
    p.add_argument('--kafka-topic', default='orders-topic')

    return p.parse_args()


def main():
    args = parse_args()

    seed = None if args.no_seed else args.seed
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    # Generate base datasets
    customers = generate_customers(num_customers=args.num_customers, seed=seed)
    products = generate_products(num_products=args.num_products, seed=seed)

    if args.mode == 'batch':
        orders = generate_orders(num_orders=args.num_orders, customers=customers, products=products, seed=seed)

        save_to_csv(customers, BATCH_CUSTOMERS_FILE, ['customer_id', 'name', 'email', 'phone', 'city', 'state', 'signup_date'])
        save_to_csv(products, BATCH_PRODUCTS_FILE, ['product_id', 'product_name', 'category', 'brand', 'price', 'stock_quantity'])
        order_fields = ['order_id', 'customer_id', 'product_id', 'quantity', 'order_timestamp', 'payment_method', 'status', 'shipping_city', 'order_value']
        save_to_csv(orders, BATCH_ORDERS_FILE, order_fields)

        print('\nBatch generation complete. Files:')
        print(' -', BATCH_CUSTOMERS_FILE)
        print(' -', BATCH_PRODUCTS_FILE)
        print(' -', BATCH_ORDERS_FILE)

    elif args.mode == 'stream-file':
        # Ensure initial batch files exist so consumers can reference product/customer metadata
        save_to_csv(customers, BATCH_CUSTOMERS_FILE, ['customer_id', 'name', 'email', 'phone', 'city', 'state', 'signup_date'])
        save_to_csv(products, BATCH_PRODUCTS_FILE, ['product_id', 'product_name', 'category', 'brand', 'price', 'stock_quantity'])

        stream_append_orders_loop(start_id=1, frequency_seconds=args.frequency, customers=customers, products=products, seed=seed, outfile=STREAM_ORDERS_FILE)

    elif args.mode == 'kafka':
        if not KAFKA_AVAILABLE:
            raise SystemExit('kafka-python not available. Install with `pip install kafka-python` to use kafka mode')

        # Optionally write metadata files for consumers
        save_to_csv(customers, BATCH_CUSTOMERS_FILE, ['customer_id', 'name', 'email', 'phone', 'city', 'state', 'signup_date'])
        save_to_csv(products, BATCH_PRODUCTS_FILE, ['product_id', 'product_name', 'category', 'brand', 'price', 'stock_quantity'])

        kafka_produce_orders(bootstrap_servers=args.kafka_bootstrap, topic=args.kafka_topic, customers=customers, products=products, frequency_seconds=args.frequency, seed=seed)


if __name__ == '__main__':
    main()
