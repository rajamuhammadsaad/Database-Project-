from pymongo import MongoClient
from pathlib import Path
import time

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
database_names = ["dataset1", "dataset2", "dataset3", "dataset4"]

# The output directory path
output_directory = r"C:\Users\rajas\Desktop\Execution Times\Mongodb"


def query_1(collection):
    start_time = time.time()
    result = list(collection.distinct("book_author")[:50])
    return round((time.time() - start_time) * 1000, 3)

def query_2(collection):
    start_time = time.time()
    result = list(collection.find({"borrow_date": {"$exists": True}}, {"_id": 0, "book_title": 1, "borrow_date": 1}))
    return round((time.time() - start_time) * 1000, 3)

def query_3(collection):
    start_time = time.time()
    result = list(collection.find({}, {"_id": 0, "book_title": 1, "borrow_date": 1, "book_isbn": 1}).limit(50))
    return round((time.time() - start_time) * 1000, 3)

def query_4(collection):
    start_time = time.time()
    result = list(collection.aggregate([
        {"$group": {"_id": "$book_author", "num_books": {"$sum": 1}}}
    ]))
    return round((time.time() - start_time) * 1000, 3)

# Execute queries 30 times for each dataset
for dataset_path in ["C:/datasets/1million_books.csv", "C:/datasets/250k_books.csv", "C:/datasets/500k_books.csv",
                     "C:/datasets/750k_books.csv"]:
    dataset_name = Path(dataset_path).stem
    db = client[dataset_name]
    collection = db["library"]

    queries = [query_1, query_3, query_2, query_4]

    for query_func in queries:
        #separate text file for each query for each dataset
        output_file = Path(output_directory) / f"{query_func.__name__}_execution_times_{dataset_name}.txt"

        print(f"\n{query_func.__name__} execution times for {dataset_name}:")
        execution_times = []

        # execute query 30 times
        for _ in range(30):
            execution_time = query_func(collection)

            # Check for zero execution time
            if execution_time > 0:
                execution_times.append(execution_time)

        # to save execution times text file
        with open(output_file, mode='w') as file:
            file.write(f"{query_func.__name__} execution times: {execution_times}\n")

        print(f"{query_func.__name__} execution times saved to {output_file}")

client.close()
