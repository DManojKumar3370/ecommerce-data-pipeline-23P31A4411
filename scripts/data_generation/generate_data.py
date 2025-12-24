"""
Data Generation Module for E-Commerce Analytics Platform
Generates realistic synthetic data for customers, products, transactions, and transaction items.
"""

import pandas as pd
import numpy as np
from faker import Faker
import json
from datetime import datetime, timedelta
import random
import os
from pathlib import Path
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Faker for consistent data generation
fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500
NUM_TRANSACTIONS = 10000
DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2024, 12, 31)

# Payment methods
PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'UPI', 'Cash on Delivery', 'Net Banking']

# Product categories
CATEGORIES = {
    'Electronics': {'subcats': ['Phones', 'Laptops', 'Tablets', 'Accessories'], 'price_range': (500, 50000)},
    'Clothing': {'subcats': ['Men', 'Women', 'Kids'], 'price_range': (200, 5000)},
    'Home & Kitchen': {'subcats': ['Appliances', 'Cookware', 'Bedding'], 'price_range': (500, 15000)},
    'Books': {'subcats': ['Fiction', 'Non-Fiction', 'Academic'], 'price_range': (100, 2000)},
    'Sports': {'subcats': ['Equipment', 'Clothing', 'Accessories'], 'price_range': (500, 10000)},
    'Beauty': {'subcats': ['Skincare', 'Makeup', 'Haircare'], 'price_range': (200, 5000)}
}


def generate_customers(num_customers: int) -> pd.DataFrame:
    """Generate customer data."""
    logger.info(f"Generating {num_customers} customers...")
    
    customers = []
    for i in range(num_customers):
        customer_id = f"CUST{i+1:04d}"
        
        customers.append({
            'customer_id': customer_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number()[:15],
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'city': fake.city(),
            'state': fake.state(),
            'country': 'India',
            'age_group': random.choice(['18-25', '25-35', '35-50', '50+'])
        })
    
    df_customers = pd.DataFrame(customers)
    df_customers = df_customers.drop_duplicates(subset=['email'], keep='first')
    
    logger.info(f"✓ Generated {len(df_customers)} unique customers")
    return df_customers


def generate_products(num_products: int) -> pd.DataFrame:
    """Generate product data."""
    logger.info(f"Generating {num_products} products...")
    
    products = []
    product_idx = 0
    
    for category, cat_info in CATEGORIES.items():
        items_per_category = num_products // len(CATEGORIES)
        
        for i in range(items_per_category):
            if product_idx >= num_products:
                break
                
            product_id = f"PROD{product_idx+1:04d}"
            subcategory = random.choice(cat_info['subcats'])
            
            # Generate price and cost (cost < price for profit)
            price = round(random.uniform(cat_info['price_range'][0], cat_info['price_range'][1]), 2)
            cost = round(price * random.uniform(0.4, 0.7), 2)
            
            products.append({
                'product_id': product_id,
                'product_name': f"{fake.word().capitalize()} {subcategory}",
                'category': category,
                'sub_category': subcategory,
                'price': price,
                'cost': cost,
                'brand': fake.word().capitalize(),
                'stock_quantity': random.randint(10, 1000),
                'supplier_id': f"SUP{random.randint(1, 50):03d}"
            })
            
            product_idx += 1
    
    df_products = pd.DataFrame(products)
    logger.info(f"✓ Generated {len(df_products)} products")
    return df_products


def generate_transactions(num_transactions: int, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Generate transaction data."""
    logger.info(f"Generating {num_transactions} transactions...")
    
    transactions = []
    customer_ids = customers_df['customer_id'].tolist()
    
    for i in range(num_transactions):
        transaction_id = f"TXN{i+1:05d}"
        customer_id = random.choice(customer_ids)  # Maintains referential integrity
        
        transaction_date = fake.date_between(start_date=DATE_START, end_date=DATE_END)
        transaction_time = fake.time()
        
        transactions.append({
            'transaction_id': transaction_id,
            'customer_id': customer_id,
            'transaction_date': transaction_date,
            'transaction_time': transaction_time,
            'payment_method': random.choice(PAYMENT_METHODS),
            'shipping_address': f"{fake.street_address()}, {fake.city()}",
            'total_amount': 0.0
        })
    
    df_transactions = pd.DataFrame(transactions)
    logger.info(f"✓ Generated {len(df_transactions)} transactions")
    return df_transactions


def generate_transaction_items(transactions_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """Generate transaction items data."""
    logger.info(f"Generating transaction items...")
    
    transaction_items = []
    product_ids = products_df['product_id'].tolist()
    product_prices = dict(zip(products_df['product_id'], products_df['price']))
    
    item_id = 1
    transaction_totals = {}
    
    for idx, row in transactions_df.iterrows():
        transaction_id = row['transaction_id']
        num_items = random.randint(1, 5)
        transaction_total = 0.0
        
        for _ in range(num_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            unit_price = product_prices[product_id]
            discount_percentage = random.choice([0, 5, 10, 15, 20])
            
            # CRITICAL: Correct line_total calculation
            line_total = round(quantity * unit_price * (1 - discount_percentage / 100), 2)
            
            transaction_items.append({
                'item_id': f"ITEM{item_id:05d}",
                'transaction_id': transaction_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percentage': discount_percentage,
                'line_total': line_total
            })
            
            transaction_total += line_total
            item_id += 1
        
        transaction_totals[transaction_id] = transaction_total
    
    df_transaction_items = pd.DataFrame(transaction_items)
    transactions_df['total_amount'] = transactions_df['transaction_id'].map(transaction_totals)
    
    logger.info(f"✓ Generated {len(df_transaction_items)} transaction items")
    return df_transaction_items


def validate_referential_integrity(customers_df: pd.DataFrame, products_df: pd.DataFrame,
                                   transactions_df: pd.DataFrame, items_df: pd.DataFrame) -> dict:
    """Validate referential integrity of generated data."""
    logger.info("Validating referential integrity...")
    
    validation_result = {
        'orphan_records': 0,
        'constraint_violations': 0,
        'quality_score': 100,
        'details': {}
    }
    
    # Check 1: customer_id in transactions exist in customers
    invalid_customers = transactions_df[~transactions_df['customer_id'].isin(customers_df['customer_id'])]
    if len(invalid_customers) > 0:
        validation_result['orphan_records'] += len(invalid_customers)
        validation_result['details']['invalid_transactions'] = len(invalid_customers)
        logger.warning(f"Found {len(invalid_customers)} transactions with invalid customer_id")
    
    # Check 2: transaction_id and product_id in items exist
    invalid_transactions = items_df[~items_df['transaction_id'].isin(transactions_df['transaction_id'])]
    if len(invalid_transactions) > 0:
        validation_result['orphan_records'] += len(invalid_transactions)
        logger.warning(f"Found {len(invalid_transactions)} items with invalid transaction_id")
    
    invalid_products = items_df[~items_df['product_id'].isin(products_df['product_id'])]
    if len(invalid_products) > 0:
        validation_result['orphan_records'] += len(invalid_products)
        logger.warning(f"Found {len(invalid_products)} items with invalid product_id")
    
    # Check 3: Business logic - line_total calculations
    errors = []
    for idx, row in items_df.iterrows():
        expected = round(row['quantity'] * row['unit_price'] * (1 - row['discount_percentage'] / 100), 2)
        if expected != row['line_total']:
            errors.append(idx)
    
    if errors:
        validation_result['constraint_violations'] += len(errors)
        logger.warning(f"Found {len(errors)} calculation errors in line_total")
    
    # Check 4: All prices positive
    negative_prices = products_df[products_df['price'] <= 0]
    if len(negative_prices) > 0:
        validation_result['constraint_violations'] += len(negative_prices)
        logger.warning(f"Found {len(negative_prices)} products with non-positive price")
    
    if validation_result['orphan_records'] == 0 and validation_result['constraint_violations'] == 0:
        validation_result['quality_score'] = 100
        logger.info("✓ All validations passed! Quality Score: 100/100")
    else:
        total_records = len(customers_df) + len(products_df) + len(transactions_df) + len(items_df)
        violations = validation_result['orphan_records'] + validation_result['constraint_violations']
        validation_result['quality_score'] = max(0, 100 - (violations / total_records * 100))
        logger.warning(f"Quality Score: {validation_result['quality_score']:.1f}/100")
    
    return validation_result


def save_data_to_csv(customers_df: pd.DataFrame, products_df: pd.DataFrame,
                     transactions_df: pd.DataFrame, items_df: pd.DataFrame, 
                     output_dir: str = 'data/raw'):
    """Save generated data to CSV files."""
    logger.info(f"Saving data to {output_dir}...")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    customers_df.to_csv(f'{output_dir}/customers.csv', index=False)
    products_df.to_csv(f'{output_dir}/products.csv', index=False)
    transactions_df.to_csv(f'{output_dir}/transactions.csv', index=False)
    items_df.to_csv(f'{output_dir}/transaction_items.csv', index=False)
    
    logger.info(f"✓ Data saved to CSV files")


def save_metadata(customers_df: pd.DataFrame, products_df: pd.DataFrame,
                  transactions_df: pd.DataFrame, items_df: pd.DataFrame,
                  validation_result: dict, output_dir: str = 'data/raw'):
    """Save generation metadata."""
    metadata = {
        'generation_timestamp': datetime.now().isoformat(),
        'record_counts': {
            'customers': len(customers_df),
            'products': len(products_df),
            'transactions': len(transactions_df),
            'transaction_items': len(items_df),
            'total_records': len(customers_df) + len(products_df) + len(transactions_df) + len(items_df)
        },
        'date_range': {
            'start_date': str(DATE_START.date()),
            'end_date': str(DATE_END.date())
        },
        'validation_result': validation_result
    }
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    with open(f'{output_dir}/generation_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info("✓ Metadata saved")


def main():
    """Main function to generate all data."""
    logger.info("=" * 60)
    logger.info("Starting E-Commerce Data Generation")
    logger.info("=" * 60)
    
    try:
        # Generate all data
        customers = generate_customers(NUM_CUSTOMERS)
        products = generate_products(NUM_PRODUCTS)
        transactions = generate_transactions(NUM_TRANSACTIONS, customers)
        items = generate_transaction_items(transactions, products)
        
        # Validate
        validation = validate_referential_integrity(customers, products, transactions, items)
        
        # Save
        save_data_to_csv(customers, products, transactions, items)
        save_metadata(customers, products, transactions, items, validation)
        
        logger.info("=" * 60)
        logger.info("✓ Data Generation Complete!")
        logger.info("=" * 60)
        
        return {
            'customers': customers,
            'products': products,
            'transactions': transactions,
            'items': items,
            'validation': validation
        }
        
    except Exception as e:
        logger.error(f"Error during data generation: {str(e)}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
