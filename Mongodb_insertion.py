import pandas as pd
import os
from pymongo import MongoClient

# MongoDB connection parameters
mongo_client = MongoClient('localhost', 27017)

# MongoDB database name
database_name = "library"

# Switch to the 'library' database
db = mongo_client[database_name]

# Absolute paths to the CSV files
csv_files = [
    r"C:\datasets\250k_books.csv",
    r"C:\datasets\500k_books.csv",
    r"C:\datasets\750k_books.csv",
    r"C:\datasets\1million_books.csv",
]

try:
    for csv_file in csv_files:
        # Generate collection name from CSV file name
        collection_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Read CSV file
        df = pd.read_csv(csv_file)

        # Use DataFrame to_dict method to convert data to a dictionary
        data = df.to_dict(orient='records')

        # Insert or update data in MongoDB collection
        db[collection_name].insert_many(data, ordered=False)

        print(f"Data from {csv_file} inserted/updated successfully into collection: {collection_name}")

except Exception as e:
    print(f"Error inserting/updating data into MongoDB: {str(e)}")

finally:
    mongo_client.close()
