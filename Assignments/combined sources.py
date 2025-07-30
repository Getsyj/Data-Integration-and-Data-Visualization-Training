import pandas as pd
import sqlite3
import json

# 1. Load CSV 
df_csv = pd.read_csv("data_csv.csv")
df_csv.rename(columns={
    "ID": "OrderID",
    "Name": "Customer",
    "Item": "Product",
    "Count": "Quantity",
    "PricePerUnit": "UnitPrice",
    "Date": "Date"
}, inplace=True)
df_csv["Source"] = "CSV"

# 2. Load Excel 
df_excel1 = pd.read_excel("data_excel.xlsx", skiprows=0, nrows=2)
df_excel2 = pd.read_excel("data_excel.xlsx", skiprows=3, nrows=2)
df_excel3 = pd.read_excel("data_excel.xlsx", skiprows=6, nrows=2)
df_excel = pd.concat([df_excel1, df_excel2, df_excel3], ignore_index=True)
df_excel.rename(columns={
    "Order": "OrderID",
    "ClientName": "Customer",
    "ProductName": "Product",
    "QuantityOrdered": "Quantity",
    "Rate": "UnitPrice",
    "OrderDate": "Date"
}, inplace=True)
df_excel["Source"] = "Excel"

# 3. Load JSON 
with open("data_json.json") as f:
    json_data = json.load(f)
df_json = pd.DataFrame(json_data)
df_json.rename(columns={
    "order_id": "OrderID",
    "customer": "Customer",
    "product": "Product",
    "qty": "Quantity",
    "price": "UnitPrice",
    "order_date": "Date"
}, inplace=True)
df_json["Source"] = "JSON"

# 4. Load SQLite 
conn = sqlite3.connect("data_orders.sqlite")
df_sql = pd.read_sql_query("SELECT * FROM WarehouseOrders", conn)
df_sql.rename(columns={
    "order_number": "OrderID",
    "client_name": "Customer",
    "item": "Product",
    "quantity": "Quantity",
    "unit_price": "UnitPrice",
    "date": "Date"
}, inplace=True)
df_sql["Source"] = "SQLite"

# 5. API 
df_dict = pd.DataFrame({
    "OrderID": [501, 502],
    "Customer": ["Ivy", "Jack"],
    "Product": ["Tablet", "Laptop"],
    "Quantity": [2, 1],
    "UnitPrice": [650, 1250],
    "Date": pd.to_datetime(["2024-05-02", "2024-05-03"])
})
df_dict["Source"] = "API"

# Combine all DataFrames
df_combined = pd.concat([df_csv, df_excel, df_json, df_sql, df_dict], ignore_index=True)

# Add derived columns
df_combined["TotalAmount"] = df_combined["Quantity"] * df_combined["UnitPrice"]
df_combined["Month"] = pd.to_datetime(df_combined["Date"]).dt.month

# Data Quality Check
total_records = len(df_combined)
missing_values = df_combined[["Product", "Quantity", "UnitPrice"]].isnull().sum()

# Handle missing values
df_cleaned = df_combined.copy()
df_cleaned = df_cleaned.dropna(subset=["Product"])
df_cleaned["Quantity"] = df_cleaned["Quantity"].fillna(df_cleaned["Quantity"].median())
df_cleaned["UnitPrice"] = df_cleaned["UnitPrice"].fillna(df_cleaned["UnitPrice"].median())

# Revenue by Source
revenue_by_source = df_cleaned.groupby("Source")["TotalAmount"].sum()

# Quantity sold per product
qty_per_product = df_cleaned.groupby("Product")["Quantity"].sum()

# Month with highest revenue
month_revenue = df_cleaned.groupby("Month")["TotalAmount"].sum()
best_month = month_revenue.idxmax()

# Top 3 customers by total purchase value
top_customers = df_cleaned.groupby("Customer")["TotalAmount"].sum().sort_values(ascending=False).head(3)

# Product with highest revenue + number of orders
product_revenue = df_cleaned.groupby("Product").agg({
    "TotalAmount": "sum",
    "OrderID": "count"
}).sort_values("TotalAmount", ascending=False)
top_product = product_revenue.iloc[0]

# Average Order Value (AOV)
aov = df_cleaned["TotalAmount"].mean()

# Top 5 UnitPrice orders
top_unitprice_orders = df_cleaned.sort_values("UnitPrice", ascending=False).head(5)

# Outlier detection using standard deviation
unitprice_mean = df_cleaned["UnitPrice"].mean()
unitprice_std = df_cleaned["UnitPrice"].std()
outlier_threshold = unitprice_mean + 3 * unitprice_std
outliers = df_cleaned[df_cleaned["UnitPrice"] > outlier_threshold]

# Export to Excel
df_cleaned.to_excel("Final_Combined_Data.xlsx", index=False)

# Print results
print("Total Records:", total_records)
print("Missing Values:\n", missing_values)
print("\nRevenue by Source:\n", revenue_by_source)
print("\nQuantity Sold per Product:\n", qty_per_product)
print("\nBest Month for Revenue:", best_month)
print("\nTop 3 Customers:\n", top_customers)
print("\nTop Product:\n", top_product)
print("\nAverage Order Value (AOV):", aov)
print("\nTop 5 Orders with Highest Unit Price:\n", top_unitprice_orders[["OrderID", "Product", "UnitPrice"]])
print("\nOutliers Detected:\n", outliers[["OrderID", "Product", "UnitPrice"]])
