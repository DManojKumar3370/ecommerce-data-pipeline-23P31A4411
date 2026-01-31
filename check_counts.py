import pandas as pd

customers = pd.read_csv("data/raw/customers.csv")
products = pd.read_csv("data/raw/products.csv")
transactions = pd.read_csv("data/raw/transactions.csv")
transaction_items = pd.read_csv("data/raw/transaction_items.csv")

print("customers:", len(customers))
print("products:", len(products))
print("transactions:", len(transactions))
print("transaction_items:", len(transaction_items))
print("total_records:", len(customers) + len(products) + len(transactions) + len(transaction_items))
