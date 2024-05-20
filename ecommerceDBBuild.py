import connection
import pandas as pd

cur, conn = connection.get_connection()

# folder_path = '/Users/kirafujibayashi/Library/Mobile Documents/com~apple~CloudDocs/Documents/UChicago/Databases/Final Project/data_clean/final_ecommerceDB' 
folder_path = '/Users/dheer/Desktop/Projects/MPCS53001/e-commerceDBproject/clean_data-20240519T203307Z-001/clean_data'

# Read in the csv files into python 
ecommerce_df = pd.read_csv(f"{folder_path}/ecommerce.csv", encoding='ISO-8859-1', low_memory=False)
customers_df = pd.read_csv(f"{folder_path}/ecommerce_customers.csv", encoding='ISO-8859-1', low_memory=False)

purchases_df = pd.read_csv(f"{folder_path}/ecommerce_purchases.csv", encoding='ISO-8859-1', low_memory=False)
orders_df = pd.read_csv(f"{folder_path}/ecommerce_order.csv", encoding='ISO-8859-1', low_memory=False)
order_details_df = pd.read_csv(f"{folder_path}/ecommerce_order_details.csv", encoding='ISO-8859-1', low_memory=False)

suppliers_df = pd.read_csv(f"{folder_path}/ecommerce_supplier.csv", encoding='ISO-8859-1', low_memory=False)
suppliers_df = suppliers_df.rename(columns={'Ã¯Â»Â¿Supplier_Name': 'Supplier_Name'})

products_df = pd.read_csv(f"{folder_path}/ecommerce_products.csv", encoding='ISO-8859-1', low_memory=False)
stores_df = pd.read_csv(f"{folder_path}/ecommerce_stores.csv", encoding='ISO-8859-1', low_memory=False)
inventory_df = pd.read_csv(f"{folder_path}/ecommerce_inventory.csv", encoding='ISO-8859-1', low_memory=False)

# Reordering DataFrame columns if necessary
ecommerce_df = ecommerce_df[['Ecommerce_ID', 'Name', 'url']]
customers_df = customers_df[['Customer_ID', 'Name_First', 'Name_Last', 'Username', 'Password', 'Address_Street', 'Address_City', 'Address_State', 'Address_Zipcode', 'Phone_Number']]
purchases_df = purchases_df[['Ecommerce_ID', 'Customer_ID']]
orders_df = orders_df[['Order_ID', 'Ecommerce_ID', 'Customer_ID', 'Date_Placed', 'status']]
order_details_df = order_details_df[['Order_ID', 'Order_Detail_ID', 'Payment_Method', 'Payment_Total_Paid', 'Order_Status', 'Start_City', 'End_City', 'Tracking_Number']]
suppliers_df = suppliers_df[['Supplier_ID', 'Supplier_Name', 'Address_Street', 'Address_City', 'Address_State', 'Address_Zipcode', 'Phone_Number', 'Date_Placed']]
products_df = products_df[['Product_ID', 'Description', 'Number_Photos', 'Price', 'Supplier_ID']]
stores_df = stores_df[['Inventory_ID', 'Product_ID']]
inventory_df = inventory_df[['Inventory_ID', 'Ecommerce_ID', 'Quantity', 'ProductName']]

# Insert into MyHotel DataBase 
# SQL query for inserting data
# Ecommerce Table
ecommerce_query = """
INSERT INTO Ecommerce (Ecommerce_ID, Name, url)
VALUES (%s, %s, %s)
"""
# Convert DataFrame to list of tuples for insertion
ecommerce_values = ecommerce_df.to_records(index=False).tolist()

# Customers Table
customers_query = """
INSERT INTO Customer (Customer_ID, Name_First, Name_Last, UserName, Password, Address_Street, Address_City, Address_State, Address_Zipcode, Phone_Number)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
customers_values = customers_df.to_records(index=False).tolist()

# Purchases Table
purchases_query = """
INSERT INTO Purchases (Ecommerce_ID, Customer_ID)
VALUES (%s, %s)
"""
purchases_values = purchases_df.to_records(index=False).tolist()

# Orders Table
orders_query = """
INSERT INTO Orders (Order_ID, Ecommerce_ID, Customer_ID, Date_Placed, Status)
VALUES (%s, %s, %s, %s, %s)
"""
orders_values = orders_df.to_records(index=False).tolist()

# Order Details Table
order_details_query = """
INSERT INTO Order_Details (Order_ID, Order_Detail_ID, Payment_Method, Payment_Total_Paid, Order_Status, Start_City, End_City, Tracking_Number)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
order_details_values = order_details_df.to_records(index=False).tolist()

# Suppliers Table
suppliers_query = """
INSERT INTO Supplier (Supplier_ID, Supplier_Name, Address_Street, Address_City, Address_State, Address_Zipcode, Phone_Number, Date_Placed)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
suppliers_values = suppliers_df.to_records(index=False).tolist()

# Products Table
products_query = """
INSERT INTO Product (Product_ID, Description, Number_Photos, Price, Supplier_ID)
VALUES (%s, %s, %s, %s, %s)
"""
products_values = products_df.to_records(index=False).tolist()

# Stores Table
stores_query = """
INSERT INTO Stores (Inventory_ID, Product_ID)
VALUES (%s, %s)
"""
stores_values = stores_df.to_records(index=False).tolist()

# Inventory Table
inventory_query = """
INSERT INTO Inventory (Inventory_ID, Ecommerce_ID, Quantity, ProductName)
VALUES (%s, %s, %s, %s)
"""
inventory_values = inventory_df.to_records(index=False).tolist()

# Check if there are any NaN values in the DataFrame
# has_nan = suppliers_df.isna().values.any()
# print("Does the DataFrame contain NaN values?", has_nan)

# # Find rows where any cell in the row has NaN
# nan_rows = suppliers_df[suppliers_df.isna().any(axis=1)]
# print("Rows with NaN values:")
# print(nan_rows)

# Insert data
cur.executemany(ecommerce_query, ecommerce_values)
cur.executemany(customers_query, customers_values)
cur.executemany(purchases_query, purchases_values)
cur.executemany(orders_query, orders_values)
cur.executemany(order_details_query, order_details_values)
cur.executemany(suppliers_query, suppliers_values)
cur.executemany(products_query, products_values)
cur.executemany(inventory_query, inventory_values)
cur.executemany(stores_query, stores_values)

# Commit the transaction
conn.commit()

# Close the connection
cur.close()
conn.close()
