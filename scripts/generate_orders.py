import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Load your existing files to get real IDs
customers = pd.read_csv('data/raw/customers.csv')
products = pd.read_csv('data/raw/products.csv')

n_customers = len(customers)
n_products = len(products)
n_orders = 5000  # or more

np.random.seed(42)
random.seed(42)

orders_data = []
start_date = datetime(2025, 1, 1)
statuses = ['COMPLETED', 'PENDING', 'CANCELLED', 'SHIPPED']
payments = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
countries = ['USA', 'India', 'UK', 'Canada', 'Australia', 'Germany', 'France']  # from your customers.csv

for i in range(1, n_orders + 1):
    order_date = start_date + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    customer_id = random.randint(1, n_customers)
    product_id = random.randint(1, n_products)
    quantity = random.randint(1, 10)
    
    # Get real unit_price from products (or fallback)
    unit_price = products[products['product_id'] == product_id]['unit_price'].iloc[0] if len(products[products['product_id'] == product_id]) > 0 else round(random.uniform(10, 100), 2)
    
    status = random.choices(statuses, weights=[0.7, 0.15, 0.1, 0.05])[0]
    total_amount = quantity * unit_price if status != 'CANCELLED' else 0.0
    payment = random.choice(payments)
    shipping_country = random.choice(countries)
    
    orders_data.append({
        'order_id': i,
        'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
        'customer_id': customer_id,
        'product_id': product_id,
        'quantity': quantity,
        'unit_price': unit_price,
        'currency': 'USD',
        'status': status,
        'total_amount': round(total_amount, 2),
        'payment_method': payment,
        'shipping_country': shipping_country
    })

df = pd.DataFrame(orders_data)
df.to_csv('data/raw/orders.csv', index=False, sep=';')  # or sep=','
print(f"Generated {len(df)} orders. Head:\n{df.head()}")
