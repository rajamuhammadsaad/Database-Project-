import mysql.connector
from pathlib import Path
import time

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="640388",
    database="library"
)
cursor = connection.cursor()

# query functions
def query_1(cursor):
    cursor.execute("USE library")
    start_time = time.time()
    cursor.execute("""
        SELECT DISTINCT book_author
        FROM 250k_books
        LIMIT 50
    """)
    result = cursor.fetchall()
    return round((time.time() - start_time) * 1000, 3)

def query_2(cursor):
    cursor.execute("USE library")
    start_time = time.time()
    cursor.execute("""
        SELECT borrow_date, book_isbn
        FROM 250k_books
        WHERE book_title = 'Book 50'
    """)
    result = cursor.fetchall()
    return round((time.time() - start_time) * 1000, 3)

def query_3(cursor):
    cursor.execute("USE library")
    start_time = time.time()
    cursor.execute("""
        SELECT *
        FROM 250k_books
        WHERE borrow_date IS NOT NULL
        LIMIT 100
    """)
    result = cursor.fetchall()
    return round((time.time() - start_time) * 1000, 3)

def query_4(cursor):
    cursor.execute("USE library")
    start_time = time.time()
    cursor.execute("""
        SELECT book_author, COUNT(*) as num_books
        FROM 250k_books
        GROUP BY book_author
    """)
    result = cursor.fetchall()
    return round((time.time() - start_time) * 1000, 3)

# The output directory path
output_directory = r"C:\Users\rajas\Desktop\Execution Times\MySQL"

# Execute queries 30 times for each dataset
for dataset_path in ["C:/datasets/250k_books.csv", "C:/datasets/500k_books.csv",
                     "C:/datasets/750k_books.csv", "C:/datasets/1million_books.csv"]:
    dataset_name = Path(dataset_path).stem

    queries = [query_1, query_2, query_3, query_4]

    for query_func in queries:
        # Create a separate text file for each query for each dataset
        output_file = Path(output_directory) / f"{query_func.__name__}_execution_times_{dataset_name}.txt"

        print(f"\n{query_func.__name__} execution times for {dataset_name}:")
        execution_times = []

        # Execute the query 30 times
        for _ in range(30):
            execution_time = query_func(cursor)

            # Check for zero execution time
            if execution_time > 0:
                execution_times.append(execution_time)

        # Save execution times to a separate text file
        with open(output_file, mode='w') as file:0
            file.write(f"{query_func.__name__} execution times: {execution_times}\n")

        print(f"{query_func.__name__} execution times saved to {output_file}")

# Close the MySQL connection
cursor.close()
connection.close()
