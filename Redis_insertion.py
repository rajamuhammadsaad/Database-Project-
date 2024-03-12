import pandas as pd
import redis
import json
import os

# Redis connection parameters
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Absolute paths to the CSV files
csv_files = [
    r"C:\datasets\250k_books.csv",
    r"C:\datasets\500k_books.csv",
    r"C:\datasets\750k_books.csv",
    r"C:\datasets\1million_books.csv",
]

try:
    for csv_file in csv_files:
        # Generate key name from CSV file name
        key_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Read CSV file
        df = pd.read_csv(csv_file)

        # Use DataFrame to_dict method to convert data to a dictionary
        data = df.to_dict(orient='records')

        # Convert the data to JSON
        json_data = json.dumps(data)

        # Set the JSON data as the value for the key in Redis
        redis_client.set(key_name, json_data)

        print(f"Data from {csv_file} inserted/updated successfully into key: {key_name}")

        # Retrieve and print the data
        retrieved_data = redis_client.get(key_name)
        if retrieved_data:
            # Convert the retrieved data back to a Python object
            retrieved_data = json.loads(retrieved_data)
            print(f"Retrieved data from key {key_name}: {retrieved_data}")
        else:
            print(f"Key {key_name} does not exist or contains no data")

except Exception as e:
    print(f"Error inserting/updating data into Redis: {str(e)}")

finally:

    pass