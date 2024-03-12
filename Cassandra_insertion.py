from cassandra.cluster import Cluster
import pandas as pd
import os

# Cassandra connection parameters
cluster = Cluster(['localhost'])
session = cluster.connect()

# Keyspace name
keyspace_name = "library"

# Create keyspace if not exists
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = "
                "{'class': 'SimpleStrategy', 'replication_factor': 1}")

# Switch to the keyspace
session.set_keyspace(keyspace_name)

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
        table_name = "t_" + os.path.splitext(os.path.basename(csv_file))[0]  # Add a letter as a prefix

        # Read CSV file
        df = pd.read_csv(csv_file)

        # Create individual tables if they don't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} ( 
            book_title VARCHAR,
            book_author VARCHAR,
            book_isbn VARCHAR PRIMARY KEY,
            borrower_name VARCHAR,
            borrower_email VARCHAR,
            borrower_phone VARCHAR,
            borrow_date DATE,
            return_date DATE
        );
        """
        session.execute(create_table_query)

        # Insert or update data in Cassandra table
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} 
            (book_title, book_author, book_isbn, borrower_name, borrower_email, borrower_phone, borrow_date, return_date) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            session.execute(insert_query, (
                row['book_title'],
                row['book_author'],
                row['book_isbn'],
                row['borrower_name'],
                row['borrower_email'],
                row['borrower_phone'],
                pd.to_datetime(row['borrow_date']).strftime('%Y-%m-%d'),
                pd.to_datetime(row['return_date']).strftime('%Y-%m-%d')
            ))

        print(f"Data from {csv_file} inserted/updated successfully into table: {table_name}")

except Exception as e:
    print(f"Error inserting/updating data into Cassandra: {str(e)}")

finally:
    cluster.shutdown()
