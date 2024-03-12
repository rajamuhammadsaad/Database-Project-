import os
import matplotlib.pyplot as plt
import numpy as np

# Directory paths for MySQL, MongoDB, Neo4j, Cassandra, and Redis recorded execution timed files
mysql_dir = r"C:\Users\rajas\Desktop\Execution Times\MySql"
mongodb_dir = r"C:\Users\rajas\Desktop\Execution Times\Mongodb"
neo4j_dir = r"C:\Users\rajas\Desktop\Execution Times\Neo4j"
cassandra_dir = r"C:\Users\rajas\Desktop\Execution Times\Cassandra"
redis_dir = r"C:\Users\rajas\Desktop\Execution Times\Redis"

# Get queries for MongoDB from file names
mongodb_files = os.listdir(mongodb_dir)
mongodb_queries = sorted(list(set([file.split('_')[1] for file in mongodb_files if file.startswith('query')])))
dataset_sizes = sorted(list(set([file.split('_')[-2] for file in mongodb_files if file.startswith('query')])))

# Get queries for Neo4j from file names
neo4j_files = os.listdir(neo4j_dir)
neo4j_queries = sorted(list(set([file.split('_')[1] for file in neo4j_files if file.startswith('query')])))

cassandra_files = os.listdir(cassandra_dir)
cassandra_queries = sorted(list(set([file.split('_')[1] for file in cassandra_files if file.startswith('query')])))

redis_files = os.listdir(redis_dir)
redis_queries = sorted(list(set([file.split('_')[1] for file in redis_files if file.startswith('query')])))

# Start dictionaries to store data for each query
mysql_datasets = {}
mongodb_datasets = {}
neo4j_datasets = {}
cassandra_datasets = {}
redis_datasets = {}


# Function to process data from text files
def process_data(file_path):
    data = []
    with open(file_path, 'r') as f:
        content = f.read()
        data = [float(entry.strip()) for entry in content[content.find("[") + 1:content.find("]")].split(",")]
    return data


# Load MySQL data
for query in mongodb_queries:  # Using MongoDB queries for MySQL as a placeholder
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(mysql_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.extend(process_data(file_path))  # Extend the list instead of appending it
    mysql_datasets[query] = allValues_query

# Load MongoDB data
for query in mongodb_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(mongodb_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.extend(process_data(file_path))  # Extend the list instead of appending it
    mongodb_datasets[query] = allValues_query

# Load Neo4j data
for query in neo4j_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(neo4j_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.extend(process_data(file_path))  # Extend the list instead of appending it
    neo4j_datasets[query] = allValues_query

# Load Cassandra data
for query in cassandra_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(cassandra_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.extend(process_data(file_path))  # Extend the list instead of appending it
    cassandra_datasets[query] = allValues_query

# Load Redis data
for query in redis_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(redis_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.extend(process_data(file_path))  # Extend the list instead of appending it
    redis_datasets[query] = allValues_query

# Create bar charts for each database and query combination
for query in mongodb_queries:
    plt.figure(figsize=(12, 8))
    plt.title(f'Comparison of Database Performance for {query}')
    plt.xlabel('Databases')
    plt.ylabel('Average Execution Time (ms)')

    databases = ['MySQL', 'MongoDB', 'Neo4j', 'Cassandra', 'Redis']
    avg_execution_times = [np.mean(mysql_datasets[query]), np.mean(mongodb_datasets[query]),
                           np.mean(neo4j_datasets[query]), np.mean(cassandra_datasets[query]),
                           np.mean(redis_datasets[query])]

    plt.bar(databases, avg_execution_times, color=['blue', 'green', 'red', 'cyan', 'magenta'])
    plt.grid(True)
    plt.show()
