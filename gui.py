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

        'Query 2': """ SELECT p.Product_ID, p.Description, p.Number_Photos, p.Price, COUNT(od.Order_ID) AS Total_Orders, AVG(p.Price) AS Avg_Price
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

        'Query 3': """ SELECT i.Inventory_ID, p.Product_ID, p.Description, p.Price, i.Quantity, COUNT(p.Product_ID) AS Total_Products
        FROM Inventory i
        JOIN Stores st ON i.Inventory_ID = st.Inventory_ID
        JOIN Product p ON st.Product_ID = p.Product_ID
        WHERE i.Ecommerce_ID = 605775548
        GROUP BY i.Inventory_ID, p.Product_ID, p.Description, p.Price, i.Quantity
        ORDER BY Total_Products DESC
        LIMIT 10;""",
    }

    # Display the radio buttons
    options = list(queries.keys())
    choice = st.radio('Choose a query to get started', options)

    # Display the selected option
    st.write('You selected:', choice)

    # Get the selected query
    selected_query = queries[choice]

    # Get user input
    user_input = st.text_input('Enter your input')

    # Modify the selected query with the user input
    selected_query = selected_query.format(user_input)


    # Run the selected query when the button is clicked
    if st.button('Run query'):
        finalQuery = selected_query
        print(finalQuery)
        finalResult = execute_query(finalQuery)
        st.write(finalResult)

            
if __name__ == "__main__":
    main()
