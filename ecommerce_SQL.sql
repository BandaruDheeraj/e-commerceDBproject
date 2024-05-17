CREATE DATABASE ecommerceDB; 

USE ecommerceDB;

drop table Stores; 
drop table Inventory; 
drop table Product; 
drop table Supplier; 
drop table Order_Details; 
drop table Orders; 
drop table Purchases; 
drop table Customer; 
drop table Ecommerce; 

CREATE TABLE Ecommerce (
	Ecommerce_ID 	INT				NOT NULL, 
    Name 			VARCHAR(15) 	NOT NULL, 
    url  			VARCHAR(25) 	NOT NULL, 
    
    PRIMARY KEY (Ecommerce_ID)
); 

CREATE TABLE Customer (
	Customer_ID 	INT 	 		NOT NULL, 
    Name_First 		VARCHAR(50) 	, 
    Name_Last 		VARCHAR(50) 	, 
    UserName	 	VARCHAR(50)		, 
    Password		VARCHAR(50)		, 
    Address_Street  VARCHAR(50)		, 
    Address_City	VARCHAR(50)		, 
    Address_State	VARCHAR(50)		, 
    Address_Zipcode INT				,
    Phone_Number	BIGINT		    , 
    
    PRIMARY KEY (Customer_ID)
); 

CREATE TABLE Purchases (
	Ecommerce_ID		INT		NOT NULL,
    Customer_ID 		INT		NOT NULL,
    
    PRIMARY KEY (Ecommerce_ID, Customer_ID),
    FOREIGN KEY (Ecommerce_ID) REFERENCES Ecommerce(Ecommerce_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customer(Customer_ID)
); 

CREATE TABLE Orders (
	Order_ID 		INT 		 NOT NULL,   
    Ecommerce_ID 	INT  		 NOT NULL,
    Customer_ID 	INT  		 NOT NULL,
    Date_Placed 	DATE		 ,
    Status		 	VARCHAR(10)  ,
    
    PRIMARY KEY (Order_ID, Ecommerce_ID, Customer_ID),
    FOREIGN KEY (Ecommerce_ID) REFERENCES Ecommerce(Ecommerce_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customer(Customer_ID)
); 

CREATE TABLE Order_Details (
	Order_ID 			INT 		 NOT NULL,   
    Order_Detail_ID		INT  		 NOT NULL,
    Payment_Method 		VARCHAR(50)  ,
    Payment_Total_Paid 	FLOAT		 ,
    Payment_Sequence	VARCHAR(5)   ,
    Order_Status		VARCHAR(10)  ,
    Start_City		 	VARCHAR(25)  ,
    End_City			VARCHAR(25)  ,
    Start_State		 	VARCHAR(20)  ,
    End_State			VARCHAR(20)  ,
    Tracking_Number		VARCHAR(25)  ,
    
    PRIMARY KEY (Order_ID, Order_Detail_ID),
    FOREIGN KEY (Order_ID) REFERENCES Orders(Order_ID)
); 

CREATE TABLE Supplier (
	Supplier_ID 		INT 		 NOT NULL,   
    Supplier_Name		VARCHAR(40)  NOT NULL,
    Address_Street  	VARCHAR(30)		, 
    Address_City		VARCHAR(20)		, 
    Address_State		VARCHAR(20)		, 
    Address_Zipcode 	INT				,
    Phone_Number		BIGINT			, 
    Date_Placed			DATE			, 
    
    PRIMARY KEY (Supplier_ID)
); 

CREATE TABLE Product (
	Product_ID 			INT 		 NOT NULL,   
    Description			VARCHAR(40)  NOT NULL,
    Number_Photos	  	INT			 , 
    Price				FLOAT 		 , 
    Supplier_ID			INT	   		 , 
    
    PRIMARY KEY (Product_ID, Supplier_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES Supplier(Supplier_ID)
); 

CREATE TABLE Inventory (
	Inventory_ID 		INT 		 NOT NULL,   
    Ecommerce_ID		INT			 NOT NULL, 
    Quantity			INT 		 ,   
    ProductName			VARCHAR(40)	 , 
    
    PRIMARY KEY (Inventory_ID, Ecommerce_ID),
    FOREIGN KEY (Ecommerce_ID) REFERENCES Ecommerce(Ecommerce_ID)
); 

CREATE TABLE Stores (
	Product_ID 			INT 		 NOT NULL,   
    Inventory_ID		INT 		 NOT NULL,   
    
    PRIMARY KEY (Product_ID, Inventory_ID),
    FOREIGN KEY (Inventory_ID) REFERENCES Inventory(Inventory_ID), 
    FOREIGN KEY (Product_ID) REFERENCES Product(Product_ID)
); 

