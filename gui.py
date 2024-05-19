import pandas as pd
import streamlit as st
import connection

def main():
    db = "ecommerceDB"

    def execute_query(query):
        # Define your connection details
        cur, conn = connection.get_connection()

        # Execute the query
        cur.execute(query)
        cur.fetchall() 

        # Fetch the results into a pandas DataFrame
        df = pd.read_sql_query(query, conn)

        # # Close the cursor and connection
        cur.close() 
        conn.close() 

        return df

    # Page title
    st.set_page_config(page_title='🦜🔗 E-Commerce App MPCS 53001')
    st.title('🦜🔗 E-Commerce App MPCS 53001')

    # Display an introduction about KQL and its use in the app
    st.write("""
    Welcome to our e-commerce store! Please select one of the following queries from below to get started
    """)
    
    # Define the queries
    queries = {
        'Retrieve all orders for a customer, including order details, product details, and supplier information, where the order starts from Chicago.': """ SELECT o.Order_ID, o.Date_Placed, o.Status, od.Start_City, od.End_City, p.Description, p.Price, s.Supplier_Name
        FROM Orders o
        JOIN Order_Details od ON o.Order_ID = od.Order_ID
        JOIN Ecommerce ec ON ec.Ecommerce_ID = o.Ecommerce_ID
        JOIN Inventory i ON ec.Ecommerce_ID = i.Ecommerce_ID
        JOIN Stores st ON i.Inventory_ID = st.Inventory_ID
        JOIN Product p ON st.Product_ID = p.Product_ID
        JOIN Supplier s ON p.Supplier_ID = s.Supplier_ID
        WHERE od.Start_City = 'Chicago'
        ORDER BY o.Date_Placed DESC
        LIMIT {}; """,

        'List all products from a specific supplier, including the total number of times each product has been ordered and the average price of these orders.': """ SELECT p.Product_ID, p.Description, p.Number_Photos, p.Price, COUNT(od.Order_ID) AS Total_Orders, AVG(p.Price) AS Avg_Price
        FROM Product p
        JOIN Supplier s ON p.Supplier_ID = s.Supplier_ID
        LEFT JOIN Stores st ON p.Product_ID = st.Product_ID
        LEFT JOIN Inventory i ON st.Inventory_ID = i.Inventory_ID
        LEFT JOIN Ecommerce ec ON i.Ecommerce_ID = ec.Ecommerce_ID
        LEFT JOIN Orders o ON ec.Ecommerce_ID = o.Ecommerce_ID
        LEFT JOIN Order_Details od ON o.Order_ID = od.Order_ID
        WHERE s.Supplier_ID = 486975277
        GROUP BY p.Product_ID, p.Description, p.Number_Photos, p.Price
        ORDER BY Total_Orders DESC; """,

        'Display all inventory items in an e-commerce store, including the total number of products in stock, grouped by product category.': """ SELECT i.Inventory_ID, p.Product_ID, p.Description, p.Price, i.Quantity, COUNT(p.Product_ID) AS Total_Products
        FROM Inventory i
        JOIN Stores st ON i.Inventory_ID = st.Inventory_ID
        JOIN Product p ON st.Product_ID = p.Product_ID
        WHERE i.Ecommerce_ID = 605775548
        GROUP BY i.Inventory_ID, p.Product_ID, p.Description, p.Price, i.Quantity
        ORDER BY Total_Products DESC
        LIMIT {};""",

        'Summarize customers by city, including the total number of customers and the total value of their orders.': """ 
        SELECT c.Address_City, COUNT(c.CustomerID) AS Total_Customers, 
            SUM(od.Payment_Total_Paid) AS Total_Order_Value
        FROM Customer c
        JOIN Orders o ON c.CustomerID = o.CustomerID
        JOIN Order_Details od ON o.Order_ID = od.Order_ID
        GROUP BY c.Address_City
        ORDER BY Total_Customers
        LIMIT {};
        """,

        ' Display the total number of orders processed by an e-commerce company on a specific date, including the total revenue generated on that date.': """ 
        SELECT ec.Name, COUNT(o.Order_ID) AS Total_Orders, 
            SUM(od.Payment_Total_Paid) AS Total_Revenue
        FROM ECommerce ec
        JOIN Orders o ON ec.Ecommerce_ID = o.Ecommerce_ID
        JOIN Order_Details od ON o.Order_ID = od.Order_ID
        WHERE o.Date_Placed = {} AND ec.Ecommerce_ID = {}
        GROUP BY ec.Name
        ORDER BY Total_Revenue
        LIMIT {};""",

        'Summarize all incomplete orders for a customer, including the total value and the average value of these orders.': """
        SELECT o.Order_ID, o.Date_Placed, o.Status, 
            SUM(od.Payment_Total_Paid) AS Total_Order_Value, 
            AVG(od.Payment_Total_Paid) AS Avg_Order_Value
        FROM Orders o
        JOIN Order_Details od ON o.Order_ID = od.Order_ID
        WHERE o.CustomerID = {}
        AND o.Status = ‘Incomplete’
        GROUP BY o.Order_ID, o.Date_Placed, o.Status
        ORDER BY Total_Order_Value DESC
        LIMIT {};""",

        'Find all customers that have made purchases from a specific supplier': """
        SELECT DISTINCT c.CustomerID, c.Name_First, c.Name_Last
        FROM Customer c
        JOIN Purchases pu ON c.CustomerID = pu.CustomerID
        JOIN E-Commerce ec ON pu.Ecommerce_ID = ec.Ecommerce_ID
        JOIN Inventory i ON ec.Ecommerce_ID = i.Ecommerce_ID
        JOIN Product p ON i.Product_ID = p.Product_ID
        JOIN Supplier s ON p.Supplier_ID = s.SupplierID
        WHERE s.SupplierID = {}
        ORDER BY c.Name_Last, c.Name_First
        LIMIT {};""",

        'Find all processed orders with a total value greater than $1000, including the customer details and the list of products in each order.': """
        SELECT o.Order_ID, o.Date_Placed, o.Status, c.Name_First, c.Name_Last, 
            SUM(od.Payment_Total_Paid) AS Total_Order_Value
        FROM Orders o
        JOIN Customer c ON o.CustomerID = c.CustomerID
        JOIN Order_Details od ON o.Order_ID = od.Order_ID
        WHERE o.Status = ‘Processed’
        GROUP BY o.Order_ID, o.Date_Placed, o.Status, c.Name_First, c.Name_Last
        HAVING SUM(od.Payment_Total_Paid) > 1000
        ORDER BY Total_Order_Value DESC
        LIMIT {};"""
    }

    # Display the radio buttons
    options = list(queries.keys())
    choice = st.radio('Choose a query to get started', options)

    # Display the selected option
    st.write('You selected:', choice)

    # Get the selected query
    selected_query = queries[choice]

  
    # Check if the selected query requires a limit
    if ' WHERE o.Date_Placed = {} AND ec.Ecommerce_ID = {}' in selected_query and 'LIMIT {}' in selected_query:
        data_placed = st.text_input('Enter the date placed for "{}"'.format(choice))
        ecommerce_id = st.text_input('Enter the e-commerce ID for "{}"'.format(choice))
        # Get user input for limit
        limit = st.number_input('Enter the amount of entries you want to see for "{}"'.format(choice), min_value=1, value=10, step=1)

        # Modify the selected query with the user input
        selected_query = selected_query.format(data_placed,ecommerce_id,limit)
    elif 'WHERE o.CustomerID = {}' in selected_query and 'LIMIT {}' in selected_query:
        customer_id = st.text_input('Enter the customer ID for "{}"'.format(choice))
        # Get user input for limit
        limit = st.number_input('Enter the amount of entries you want to see for "{}"'.format(choice), min_value=1, value=10, step=1)

        # Modify the selected query with the user input
        selected_query = selected_query.format(customer_id, limit)
    elif 'WHERE s.SupplierID = {}' in selected_query and 'LIMIT {}' in selected_query:
        supplier_id = st.text_input('Enter the supplier ID for "{}"'.format(choice))
        # Get user input for limit
        limit = st.number_input('Enter the amount of entries you want to see for "{}"'.format(choice), min_value=1, value=10, step=1)

        # Modify the selected query with the user input
        selected_query = selected_query.format(supplier_id, limit)
    elif 'LIMIT {}' in selected_query:
        # Get user input for limit
        limit = st.number_input('Enter the amount of entries you want to see for "{}"'.format(choice), min_value=1, value=10, step=1)

        # Modify the selected query with the user input
        selected_query = selected_query.format(limit)


    # Run the selected query when the button is clicked
    if st.button('Run query'):
        finalQuery = selected_query
        print(finalQuery)
        finalResult = execute_query(finalQuery)
        st.write(finalResult)

            
if __name__ == "__main__":
    main()
