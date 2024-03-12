import redis
from pathlib import Path
import time

# Connect to Redis
redis_connection = redis.StrictRedis(host='localhost', port=6379, db=0)


def query_1():
    start_time = time.time()
    result = redis_connection.lrange('books:value1:authors', 0, 49)
    return round((time.time() - start_time) * 1000, 3)

def query_2():
    start_time = time.time()
    result = redis_connection.hgetall('books:value1:borrow_info')
    return round((time.time() - start_time) * 1000, 3)

def query_3():
    start_time = time.time()
    result = redis_connection.lrange('books:value1:borrowed_books', 0, 49)
    return round((time.time() - start_time) * 1000, 3)

def query_4():
    start_time = time.time()
    result = redis_connection.hgetall('books:num_books_per_author')
    return round((time.time() - start_time) * 1000, 3)

# the output directory path
output_directory = r"C:\Users\rajas\Desktop\Execution Times\Redis"

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
            execution_time = query_func()

            # Check for zero execution time
            if execution_time > 0:
                execution_times.append(execution_time)

        # Save execution times to a separate text file
        with open(output_file, mode='w') as file:
            file.write(f"{query_func.__name__} execution times: {execution_times}\n")

        print(f"{query_func.__name__} execution times saved to {output_file}")
