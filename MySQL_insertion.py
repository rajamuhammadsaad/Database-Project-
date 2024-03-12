import mysql.connector
import pandas as pd
import os
import time

# MySQL database connection parameters
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="640388",
    port="3306",
    connect_timeout= 600,
)

# Create a cursor object
cursor = connection.cursor()

# Database name
database_name = "library"

# Check if the database exists
check_database_query = f"SHOW DATABASES LIKE '{database_name}';"
cursor.execute(check_database_query)

database_exists = False
for (database,) in cursor:
    if database == database_name:
        database_exists = True
        break

# If the database doesn't exist, create it
if not database_exists:
    create_database_query = f"CREATE DATABASE {database_name};"
    cursor.execute(create_database_query)
    print(f"Database '{database_name}' created.")

# Switch to the 'library' database
use_database_query = f"USE {database_name};"
cursor.execute(use_database_query)

# Absolute paths to the CSV files
csv_files = [
    r"C:\datasets\250k_books.csv",
    r"C:\datasets\500k_books.csv",
    r"C:\datasets\750k_books.csv",
    r"C:\datasets\1million_books.csv",
]

try:
    for csv_file in csv_files:
        # Generate table name from CSV file name
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Read CSV file
        df = pd.read_csv(csv_file)

        # Create individual tables if they don't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` ( 
            book_title VARCHAR(255),
            book_author VARCHAR(255),
            book_isbn VARCHAR(255) PRIMARY KEY,
            borrower_name VARCHAR(255),
            borrower_email VARCHAR(255),
            borrower_phone VARCHAR(255),
            borrow_date DATE,
            return_date DATE
        );
        """
        cursor.execute(create_table_query)

        # Use INSERT INTO query with ON DUPLICATE KEY UPDATE
        insert_query = f"""
        INSERT INTO `{table_name}` 
        (book_title, book_author, book_isbn, borrower_name, borrower_email, borrower_phone, borrow_date, return_date) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        book_title=VALUES(book_title), book_author=VALUES(book_author), borrower_name=VALUES(borrower_name),
        borrower_email=VALUES(borrower_email), borrower_phone=VALUES(borrower_phone),
        borrow_date=VALUES(borrow_date), return_date=VALUES(return_date);
        """
        values = df.values.tolist()

        # Attempt to execute the query with reconnection on failure
        while True:
            try:
                cursor.executemany(insert_query, values)
                print(f"Data from {csv_file} inserted/updated successfully into table: {table_name}")
                break  # Break out of the loop if successful
            except mysql.connector.errors.OperationalError as e:
                if e.errno == 2006 or e.errno == 2013:  # Lost connection errors
                    print(f"Lost connection. Reconnecting...")
                    connection.reconnect()
                    cursor = connection.cursor()
                    time.sleep(2)  # Wait before retrying
                else:
                    raise  # Re-raise other OperationalErrors

except Exception as e:
    print(f"Error inserting/updating data into tables. Error: {str(e)}")
finally:
    connection.commit()
    cursor.close()
    connection.close()
