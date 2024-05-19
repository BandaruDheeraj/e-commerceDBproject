import mysql.connector
import pandas as pd
import streamlit as st


def main():
    db = ""


    @st.cache(allow_output_mutation=True)
    def execute_query(database, query):
        # Define your connection details
        conn_details = {
            'host': 'your_host',
            'user': 'your_username',
            'password': 'your_password',
            'database': database
        }

        # Create a new connection
        conn = mysql.connector.connect(**conn_details)

        # Create a new cursor
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch the results into a pandas DataFrame
        df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])

        # Close the cursor and connection
        cursor.close()
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
        'Query 1': 'SQL query 1',
        'Query 2': 'SQL query 2',
        'Query 3': 'SQL query 3',
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
        finalResult = execute_query(db, finalQuery)
        st.write(finalResult)

            
if __name__ == "__main__":
    main()

