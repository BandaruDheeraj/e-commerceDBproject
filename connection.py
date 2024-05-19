import pymysql
import mysql.connector

def get_connection():
    conn = mysql.connector.connect(host="localhost", user="root", password="passphrase", db="ecommerceDB")
    cur = conn.cursor()
    return cur, conn

