import os
import matplotlib.pyplot as plt
import numpy as np

# Directory paths for MySQL, MongoDB, Neo4j, Cassandra, and Redis recorded execution timed files
mysql_dir = r"C:\Users\rajas\Desktop\Execution Times\MySql"
mongodb_dir = r"C:\Users\rajas\Desktop\Execution Times\Mongodb"
neo4j_dir = r"C:\Users\rajas\Desktop\Execution Times\Neo4j"
cassandra_dir = r"C:\Users\rajas\Desktop\Execution Times\Cassandra"
redis_dir = r"C:\Users\rajas\Desktop\Execution Times\Redis"


mongodb_files = os.listdir(mongodb_dir)
mongodb_queries = sorted(list(set([file.split('_')[1] for file in mongodb_files if file.startswith('query')])))
dataset_sizes = sorted(list(set([file.split('_')[-2] for file in mongodb_files if file.startswith('query')])))


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
        allValues_query.append(process_data(file_path))
    mysql_datasets[query] = allValues_query

# Load MongoDB data
for query in mongodb_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(mongodb_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.append(process_data(file_path))
    mongodb_datasets[query] = allValues_query

# Load Neo4j data
for query in neo4j_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(neo4j_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.append(process_data(file_path))
    neo4j_datasets[query] = allValues_query

# Load Cassandra data
for query in cassandra_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(cassandra_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.append(process_data(file_path))
    cassandra_datasets[query] = allValues_query

# Load Redis data
for query in redis_queries:
    allValues_query = []
    for size in dataset_sizes:
        file_path = os.path.join(redis_dir, f"query_{query}_execution_times_{size}_books.txt")
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue
        allValues_query.append(process_data(file_path))
    redis_datasets[query] = allValues_query

# Create a list of colors for queries
colors = ['b', 'g', 'r', 'c']

# Create two plots for each database (First execution and Average of 30 executions)
for db_name, db_datasets in [("MySQL", mysql_datasets), ("MongoDB", mongodb_datasets), ("Neo4j", neo4j_datasets), ("Cassandra", cassandra_datasets), ("Redis", redis_datasets)]:
    plt.figure(figsize=(12, 8))

    for i, query in enumerate(db_datasets.keys()):
        response_times = [values[0] for values in db_datasets[query]]  # First execution
        x = np.arange(len(dataset_sizes))
        plt.bar(x + i * 0.2, response_times, width=0.2, label=f'{query} (First Execution)', color=colors[i])

    plt.xlabel('Dataset Size')
    plt.ylabel('Response Time (ms)')
    plt.title(f'{db_name} Queries (First Execution)')
    plt.xticks(x + 0.2 * 1.5, dataset_sizes)
    plt.legend()
    plt.grid(True)

    plt.figure(figsize=(12, 8))

    for i, query in enumerate(db_datasets.keys()):
        average_response_times = [np.mean(values) for values in db_datasets[query]]  # Average of 30 executions
        x = np.arange(len(dataset_sizes))
        plt.bar(x + i * 0.2, average_response_times, width=0.2, label=f'{query} (Average of 30 Executions)', color=colors[i])

    plt.xlabel('Dataset Size')
    plt.ylabel('Average Response Time (ms)')
    plt.title(f'{db_name} Queries (Average of 30 Executions)')
    plt.xticks(x + 0.2 * 1.5, dataset_sizes)
    plt.legend()
    plt.grid(True)

plt.show()
