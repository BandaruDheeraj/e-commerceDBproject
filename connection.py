import mysql.connector

# Connect with the MySQL Database
def get_connection():
    # conn = mysql.connector.connect(host="localhost", user="root", password="passphrase", db="ecommerceDB") #Kira's Connection
    conn = mysql.connector.connect(host="localhost", user="root", password="test", db="ecommerceDB")  #Dheeraj's Connection

    cur = conn.cursor()
    return cur, conn

