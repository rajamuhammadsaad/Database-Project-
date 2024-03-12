from cassandra.cluster import Cluster
from pathlib import Path
import time

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('library')

output_directory = r"C:\Users\rajas\Desktop\Execution Times\Cassandra"

# Sample query functions
def query_1():
    start_time = time.time()
    result = session.execute("SELECT book_author From t_250k_books")
    return round((time.time() - start_time) * 1000, 3)

def query_2():
    start_time = time.time()
    result = session.execute("SELECT book_title FROM t_250k_books")
    return round((time.time() - start_time) * 1000, 3)

def query_3():
    start_time = time.time()
    result = session.execute("SELECT * FROM t_500k_books WHERE borrow_date >= '2020-01-01' AND borrow_date <= '2023-01-01' ALLOW FILTERING")
    return round((time.time() - start_time) * 1000, 3)

def query_4():
    start_time = time.time()
    result = session.execute("SELECT borrow_date, book_isbn FROM t_250k_books WHERE book_title = 'Book 50' ALLOW FILTERING")
    return round((time.time() - start_time) * 1000, 3)


# Execute queries 30 times for each dataset
for dataset_path in ["C:/datasets/250k_books.csv", "C:/datasets/500k_books.csv",
                     "C:/datasets/750k_books.csv", "C:/datasets/1million_books.csv"]:
    dataset_name = Path(dataset_path).stem

    queries = [query_1, query_2, query_3, query_4]

    for query_func in queries:

        output_file = Path(output_directory) / f"{query_func.__name__}_execution_times_{dataset_name}.txt"
        print(f"\n{query_func.__name__} execution times for {dataset_name}:")
        execution_times = []

        # Execute the query 30 times
        for _ in range(30):
            execution_time = query_func()

            # Check for zero execution time
            if execution_time > 0:
                execution_times.append(execution_time)

        with open(output_file, mode='w') as file:
            file.write(f"{query_func.__name__} execution times: {execution_times}\n")

        print(f"{query_func.__name__} execution times saved to {output_file}")


cluster.shutdown()
