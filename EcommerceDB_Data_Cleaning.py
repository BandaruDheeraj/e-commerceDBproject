
"""
EcommerceDB_Data_Cleaning.py: create and clean data sets for database final project 

Here is the link to the data saved the Google drive: 

The following tables/variables are mocked data created using mockaroo: 
Ecommerce: ecommerce_ID, Name, URL
Customer: CustomerID (kaggle data), Name_First, Name_Last, UserName, Password, Address_Street, Address_City, Address_State, Address_Zipcode, Phone_Number 
Purchases: Subset the orders table below to keep the random assignment of ecommerce_ID and CustomerID
Orders: randomly assigned ecommerce_ID and CustomerID
Order_Details: Tracking_Number, Start_City, End_City 
Supplier: Supplier_Name, Address_Street, Address_City, Address_State, Address_Zipcode
Product: data joined together from olist_order_items_dataset, olist_products_dataset, product_category_name_translation
Stores: randomly assigned Product_ID and Inventory_ID
Inventory: Iventory_ID, Quantity, ProductName, randomly assigned to an ecommerce_ID
"""

__author__ = "Kira Fujibayashi"
__email__ = "kirafujibayashi@uchicago.edu"

#import modules 
import pandas as pd 
from datetime import datetime

import glob 
import re
import numpy as np

file_path = ''

# declare global variable for local file path 
folder_path = '' 
out_folder_path = ''

### Define Helper Functions 
# Load Observation Records 
def load_data(extension, folder_path=folder_path): 
    '''
    Read in CSV file and return DataFrame of mocked data

    Input: csv file of observations 

    Output: DataFrame
    '''
    # Use glob to get all the CSV file paths in the folder
    file_paths = glob.glob(f"{folder_path}/{extension}/*.csv")

    # Initialize an empty list to collect DataFrames
    dataframes = []

    # Iterate over the file paths and append each CSV file to the list
    for file_path in file_paths:
        print("Loading:", file_path)
        df = pd.read_csv(file_path, encoding='ISO-8859-1', low_memory=False)
        dataframes.append(df)

    # Concatenate all DataFrames in the list into one DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Print DataFrame summary information
    print("Number of rows:", len(combined_df))
    print("Unique values per column:\n", combined_df.nunique())

    # optional: save combined datasets to csv
    combined_df.to_csv(f"{folder_path}/combined_mock_data.csv", index=False)

    return combined_df

# Remove NonNumeric Characters 
def removeNonNumericChars(df, columns):
    '''
    Modify string columns in the DataFrame to remove non-numeric characters and
    truncate numeric IDs if they exceed 2,147,483,647 by removing digits from the end.

    Input:
    - df: DataFrame to modify
    - columns: List of column names to process

    Output:
    - DataFrame with modified string columns
    '''
    max_int_value = 2147483647  # Maximum value for a 32-bit signed integer

    def truncate_numeric_id(x):
        x = re.sub(r'[^0-9]', '', str(x))  # Remove non-numeric characters first
        # Truncate digits from the end if the number is too large
        while len(x) > 0 and int(x) > max_int_value:
            x = x[:-1]
        return int(x) if x else None  # Convert back to integer, or None if empty

    for col in columns:
        if df[col].dtype == 'object':  # Assuming 'object' dtype for string columns
            # Apply both removal of non-numeric characters and truncation
            df[col] = df[col].apply(truncate_numeric_id)

    return df

def removeDashChars(df, columns):
    '''
    Modify string columns in the DataFrame to remove non-numeric characters and
    truncate numeric IDs if they exceed 2,147,483,647 by removing digits from the end.

    Input:
    - df: DataFrame to modify
    - columns: List of column names to process

    Output:
    - DataFrame with modified string columns
    '''
    max_int_value = 2147483647  # Maximum value for a 32-bit signed integer

    def truncate_numeric_id(x):
        x = re.sub(r'[^0-9]', '', str(x))  # Remove non-numeric characters first
        # Truncate digits from the end if the number is too large
        return int(x) if x else None  # Convert back to integer, or None if empty

    for col in columns:
        if df[col].dtype == 'object':  # Assuming 'object' dtype for string columns
            # Apply both removal of non-numeric characters and truncation
            df[col] = df[col].apply(truncate_numeric_id)

    return df


# Print max string length 
def print_max_string_length(df):
    '''
    Iterate through each column of the DataFrame, printing the column name and
    the maximum length of strings in that column, for columns containing string data.

    Input: DataFrame
    '''
    print(f"Table Name: {df}")
    for col in df.columns:
        if df[col].dtype == 'object':  # Assuming 'object' dtype for string columns
            # Calculate the maximum length of strings in the column
            max_length = df[col].apply(lambda x: len(str(x))).max()
            print(f"Column: {col}, Max String Length: {max_length}")


# Load Observation Records for orders and order detail tables:order_details_df = pd.read_csv(f"{folder_path}/olist_order_items_dataset.csv", encoding='ISO-8859-1', low_memory=False)
order_details_mock_df = load_data(extension = 'MockData_OrderDetails') 
customer_mock_df = load_data(extension = 'MockData_Customer') 
order_df = pd.read_csv(f"{folder_path}/olist_orders_dataset_clean.csv", encoding='ISO-8859-1', low_memory=False)
order_details_df = pd.read_csv(f"{folder_path}/olist_order_items_dataset.csv", encoding='ISO-8859-1', low_memory=False)
order_payment_df = pd.read_csv(f"{folder_path}/olist_order_payments_dataset.csv", encoding='ISO-8859-1', low_memory=False)

# Load Observation Records for product table and translated descriptions 
brazil_products_df = pd.read_csv(f"{folder_path}/olist_products_dataset.csv", encoding='ISO-8859-1', low_memory=False)
product_translation_df = pd.read_csv(f"{folder_path}/product_category_name_translation.csv", encoding='ISO-8859-1', low_memory=False)

# Remove non-numeric values from the IDs
order_df = removeNonNumericChars(order_df, ['order_id',	'customer_id'])
order_df = order_df.drop_duplicates(subset=['order_id'], keep='first')
order_details_df = removeNonNumericChars(order_details_df, ['order_id',	'order_item_id', 'product_id', 'seller_id'])
order_details_df = order_details_df.drop_duplicates(subset=['order_id'], keep='first')
order_payment_df = removeNonNumericChars(order_payment_df, ['order_id'])
order_payment_df = order_payment_df.drop_duplicates(subset=['order_id'], keep='first')

brazil_products_df = removeNonNumericChars(brazil_products_df, ['product_id'])


### Preparing the Order and Order Details Tables ### 

# join order_df and order_details_df to get only orders matched a customer ID and join in payment info
details_df = pd.merge(order_df, order_details_df, on=['order_id', 'order_id'])

# Dropping unnamed columns
details_df = details_df.drop(columns=['Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7','Unnamed: 8'])
df = pd.merge(details_df, order_payment_df, on=['order_id', 'order_id'])

print(df.head())
# df = pd.merge(orders_df, order_payment_df, on=['order_id', 'order_id'])

# Format the datetime object to 'yyyy-mm-dd'
df['Date_Placed'] = pd.to_datetime(df['order_purchase_timestamp'])
df['Date_Placed'] = df['Date_Placed'].dt.strftime('%Y-%m-%d')

# Calculate the total paid price by adding the price and freight_value 
df['Payment_Total_Paid'] = df['price']+df['freight_value']

# Create a copy of the df to use late in the product tables 
df_copy = df 

# Rename columns to match entity relationship 
df = df.drop(columns=['product_id', 'seller_id', 'shipping_limit_date', 'freight_value', 'price', 'payment_installments', 'order_status', 'order_purchase_timestamp'])
df = df.rename(columns={
    'payment_sequential': 'Payment_Sequence',
    'payment_type': 'Payment_Method', 
    'customer_id': 'customerID', 
    'order_id': 'Order_ID', 
    'payment_value': 'Installment_Payment_Amount', 
    'order_item_id': 'Order_Item_ID', 
})

# Concatenating columns as strings
df['Order_Detail_ID'] = df['Payment_Sequence'].astype(str) + df['Order_Item_ID'].astype(str)
df = df.drop_duplicates()

order_table_df = df[['Order_ID', 'Date_Placed', 'status', 'customerID']]

### Preparing the Ecommerce Table from the mockaroo.com data ###
ecommerce_ids_df = pd.read_csv(f"{folder_path}/ecommerce.csv", encoding='ISO-8859-1', low_memory=False)
ecommerce_ids_df = removeNonNumericChars(ecommerce_ids_df, ['ecommerce_ID'])

# print(ecommerce_ids_df.columns)

# Extract the Ecommerce_ID column as a list
ecommerce_id_list = ecommerce_ids_df['ecommerce_ID'].tolist()

# Randomly assign an E-commerce ID to each order
order_table_df['Ecommerce_ID'] = np.random.choice(ecommerce_id_list, size=len(order_table_df))


### Preparing the Ecommerce Table from the order table ###
purchases_df = order_table_df[['Ecommerce_ID', 'customerID']]



### Preparing the Product Tables ### 
products_df = df_copy[['product_id', 'seller_id', 'price', 'freight_value', 'Payment_Total_Paid']]

# Print Product Table Columns
print("Columns in brazil_products_df:", brazil_products_df.columns)
print("Columns in product_translation_df:", product_translation_df.columns)

# Rename the column
product_translation_df.rename(columns={'ï»¿product_category_name': 'product_category_name'}, inplace=True)
print("Updated Columns in product_translation_df:", product_translation_df.columns)

# Merge the product DataFrames to get the english translation of the product description 
br_products_combined_df = pd.merge(brazil_products_df, product_translation_df, on=['product_category_name', 'product_category_name'])
products_df = pd.merge(products_df, br_products_combined_df, on=['product_id', 'product_id'])

# Remove duplicates based on 'product_category_name' and 'product_id'
products_df = products_df.drop_duplicates(subset=['product_category_name', 'product_id'], keep='first')

products_final_df = products_df.drop(columns=['product_name_lenght','product_description_lenght', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm', 'product_category_name'])
products_final_df = products_final_df.rename(columns={
    'seller_id': 'Supplier_ID',
    'product_category_name_english': 'Description', 
    'product_photos_qty': 'Number_Photos', 
    'order_id': 'Order_ID', 
    'price': 'Price'
})

# Extract the Supplier_ID column as a list
supplier_id_list = products_final_df['Supplier_ID'].tolist()  # Assuming this is your list
unique_supplier_ids = set(supplier_id_list)  # Convert list to set to remove duplicates
count_unique = len(unique_supplier_ids)  # The number of unique elements

print("Number of unique Supplier IDs:", count_unique)

### Inventory - Join product_df to inventory mocked data ### 
inventory_ids_df = pd.read_csv(f"{folder_path}/inventory.csv", encoding='ISO-8859-1', low_memory=False)
inventory_ids_df = removeNonNumericChars(inventory_ids_df, ['Inventory_ID'])

print(products_final_df.columns)

# Extract the Ecommerce_ID column as a list
inventory_id_list = inventory_ids_df['Inventory_ID'].tolist()

# # Randomly assign an E-commerce ID to each order
products_final_df['Inventory_ID'] = np.random.choice(inventory_id_list, size=len(products_final_df))

inventory_df = pd.merge(products_final_df, inventory_ids_df, on='Inventory_ID')
print("Inventory columns:", inventory_df.columns)

# Relationship table -- stores 
stores_df = inventory_df[['Inventory_ID', 'product_id']]

inventory_df = inventory_df.rename(columns={
    'quantity ': 'Quantity',
    'Description': 'ProductName'
})
inventory_df = inventory_df[['Inventory_ID', 'Quantity', 'ProductName']]

# Randomly assign E-commerce IDs to inventory 
inventory_df['Ecommerce_ID'] = np.random.choice(ecommerce_id_list, size=len(inventory_df))

print("Inventory Columns:", inventory_df.columns)
print("Stores Columns:", stores_df.columns)

## Supplier Table -- randomly assign supplier_id from product table to the mocked data records 
supplier_mocked_df = pd.read_csv(f"{folder_path}/supplier_mocked_data.csv", encoding='ISO-8859-1', low_memory=False)
# Convert the set to a DataFrame
unique_supplier_ids_df = pd.DataFrame(list(unique_supplier_ids), columns=['Supplier_ID'])
# Now concatenate it with another DataFrame
supplier_mocked_df = pd.concat([unique_supplier_ids_df, supplier_mocked_df], axis=1)

# Convert to datetime object in pandas
supplier_mocked_df['Date_Placed'] = pd.to_datetime(supplier_mocked_df['Date_Placed'] , format='%m/%d/%y')
# Format the datetime object to 'yyyy-mm-dd'
supplier_mocked_df['Date_Placed']  = pd.to_datetime(supplier_mocked_df['Date_Placed'])
supplier_mocked_df['Date_Placed'] = supplier_mocked_df['Date_Placed'] .dt.strftime('%Y-%m-%d')

supplier_mocked_df = supplier_mocked_df.drop_duplicates(subset='Supplier_ID')
supplier_mocked_df = supplier_mocked_df.dropna()

## Save all tables to the final output 
# Ecommerce Table 
ecommerce_ids_df = ecommerce_ids_df.rename(columns={'ecommerce_ID': 'Ecommerce_ID'})
ecommerce_ids_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce.csv", index=False)

# Customer Table 
## Save customer table and join with customer_mock_df in excel to create customer records 
customer_id_df = order_df['customer_id']
customer_id_df.name = 'Customer_ID'
customer_mock_df = customer_mock_df.rename(columns={'street_address': 'Address_Street', 
                                                    'city': 'Address_City', 
                                                    'zipcode': 'Address_Zipcode', 
                                                    'state': 'Address_State', 
                                                    'phone_number': 'Phone_Number', 
                                                    'first_name': 'Name_First', 
                                                    'last_name': 'Name_Last'})

customer_id_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/customer_id_df.csv", index=False)
customer_mock_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/customer_mock_df.csv", index=False)

cust_result = pd.concat([customer_id_df, customer_mock_df], axis=1)
cust_result_cleaned = cust_result.dropna()
# Removing dashes from the phone_number column
cust_result_cleaned = removeDashChars(cust_result_cleaned, ['Phone_Number'])
# cust_result_cleaned['Phone_Number'] = cust_result_cleaned['Phone_Number'].str.replace('-', '', regex=False)
cust_result_cleaned.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_customers.csv", index=False)

# Purchase Table
purchases_df = purchases_df.rename(columns={'customerID': 'Customer_ID'})
purchases_df = purchases_df.drop_duplicates()
purchases_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_purchases.csv", index=False)

# Orders Table
order_table_df = order_table_df.rename(columns={'customerID': 'Customer_ID'})
order_table_df = order_table_df.drop_duplicates()
order_table_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_order.csv", index=False)

# Order Details Table
## Save order_details table and join with order_details_mock_df in excel to create order_details
df = df.rename(columns={'customerID': 'Customer_ID'})
dets_result = pd.concat([df, order_details_mock_df], axis=1)
dets_result = dets_result.rename(columns={'start_city': 'Start_City', 
                                        'end_city': 'End_City',
                                        'start_state': 'Start_State', 
                                        'end_state': 'End_State', 
                                        'tracking_number': 'Tracking_Number', 
                                        'status': 'Order_Status'})

order_dets_result_cleaned = dets_result.dropna()
order_dets_result_cleaned2 = order_dets_result_cleaned.drop_duplicates()
order_dets_result_cleaned2.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_order_details.csv", index=False)

# Supplier Table 
supplier_mocked_df = supplier_mocked_df.rename(columns={'√Ø¬ª¬øSupplier_Name': 'Supplier_Name'})
supplier_mocked_clean_df = removeDashChars(supplier_mocked_df, ['Phone_Number'])
supplier_mocked_clean_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_supplier.csv", index=False)

# Product Table 
products_final_df = products_final_df.rename(columns={'product_id': 'Product_ID'})
products_final_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_products.csv", index=False)

# Stores Table 
stores_df = stores_df.rename(columns={'product_id': 'Product_ID'})
stores_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_stores.csv", index=False)

# Inventory Table
inventory_df = inventory_df.drop_duplicates(subset=['Inventory_ID'], keep='first')
inventory_df.to_csv(f"/{out_folder_path}/final_ecommerceDB/ecommerce_inventory.csv", index=False)

## print the max length for loading into the database 
list_tables = [ecommerce_ids_df, purchases_df, order_table_df, order_dets_result_cleaned, df, supplier_mocked_df, products_final_df, stores_df, inventory_df]
for table in list_tables: 
    print(f"{table}")
    print_max_string_length(table)
