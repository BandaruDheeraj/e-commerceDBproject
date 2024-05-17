import mysql.connector 
import pandas as pd 

myConnection = mysql.connector.connect(user = 'root', 
                                       password = '<M6a0n6h3a7t5t9a1n3>',
                                       host = 'localhost', 
                                       database = 'ecommerceDB') 
print(myConnection)

# Open the cursor object 
cursorObject = myConnection.cursor() 

folder_path = '/Users/kirafujibayashi/Library/Mobile Documents/com~apple~CloudDocs/Documents/UChicago/Databases/Final Project/data_clean/final_ecommerceDB' 

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
orders_df = orders_df[['Order_ID', 'Ecommerce_ID', 'Customer_ID', 'Date_Placed']]
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
INSERT INTO Orders (Order_ID, Ecommerce_ID, Customer_ID, Date_Placed)
VALUES (%s, %s, %s, %s)
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
has_nan = suppliers_df.isna().values.any()
print("Does the DataFrame contain NaN values?", has_nan)

# Find rows where any cell in the row has NaN
nan_rows = suppliers_df[suppliers_df.isna().any(axis=1)]
print("Rows with NaN values:")
print(nan_rows)

# Insert data
cursorObject.executemany(ecommerce_query, ecommerce_values)
cursorObject.executemany(customers_query, customers_values)
cursorObject.executemany(purchases_query, purchases_values)
cursorObject.executemany(orders_query, orders_values)
cursorObject.executemany(order_details_query, order_details_values)
cursorObject.executemany(suppliers_query, suppliers_values)
cursorObject.executemany(products_query, products_values)
cursorObject.executemany(inventory_query, inventory_values)
cursorObject.executemany(stores_query, stores_values)

# Query One: 
stats_query = """
        SELECT o.Order_ID, o.Date_Placed, o.Status, od.Start_City, od.End_City, p.Description, p.Price, s.Supplier_Name
        FROM Orders o
        JOIN Order_Details od ON o.Order_ID = od.Order_ID
        JOIN Ecommerce ec ON ec.Ecommerce_ID = o.Ecommerce_ID
        JOIN Inventory i ON ec.Ecommerce_ID = i.Ecommerce_ID
        JOIN Stores st ON i.Inventory_ID = st.Inventory_ID
        JOIN Product p ON st.Product_ID = p.Product_ID
        JOIN Supplier s ON p.Supplier_ID = s.Supplier_ID
        WHERE od.Start_City = 'Chicago'
        ORDER BY o.Date_Placed DESC
        LIMIT 10;
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

# Query Two: 
stats_query = """
        SELECT p.Product_ID, p.Description, p.Number_Photos, p.Price, COUNT(od.Order_ID) AS Total_Orders, AVG(p.Price) AS Avg_Price
        FROM Product p
        JOIN Supplier s ON p.Supplier_ID = s.Supplier_ID
        LEFT JOIN Stores st ON p.Product_ID = st.Product_ID
        LEFT JOIN Inventory i ON st.Inventory_ID = i.Inventory_ID
        LEFT JOIN Ecommerce ec ON i.Ecommerce_ID = ec.Ecommerce_ID
        LEFT JOIN Orders o ON ec.Ecommerce_ID = o.Ecommerce_ID
        LEFT JOIN Order_Details od ON o.Order_ID = od.Order_ID
        WHERE s.Supplier_ID = 486975277
        GROUP BY p.Product_ID, p.Description, p.Number_Photos, p.Price
        ORDER BY Total_Orders DESC;
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

#Query Three: This query needs review. 
stats_query = """
        SELECT i.Inventory_ID, p.Product_ID, p.Description, p.Price, i.Quantity, COUNT(p.Product_ID) AS Total_Products
        FROM Inventory i
        JOIN Stores st ON i.Inventory_ID = st.Inventory_ID
        JOIN Product p ON st.Product_ID = p.Product_ID
        WHERE i.Ecommerce_ID = 605775548
        GROUP BY i.Inventory_ID, p.Product_ID, p.Description, p.Price, i.Quantity
        ORDER BY Total_Products DESC
        LIMIT 10;
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

#Query Four: 
stats_query = """
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

# Query Five: 
stats_query = """
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

# Query Six: 
stats_query = """
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

# Query Seven: 
stats_query = """
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

# Query Eight: 
stats_query = """
        """

cursorObject.execute(stats_query)
rows = cursorObject.fetchall()  # This fetches all rows returned by the query
stats_query_df = pd.read_sql_query(stats_query, myConnection)

print(stats_query_df)

# Commit changes and close connection
# cursorObject.commit()
cursorObject.close() 
myConnection.close() 

