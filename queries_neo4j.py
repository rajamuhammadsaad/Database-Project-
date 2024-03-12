from py2neo import Graph
from pathlib import Path
import time

# Connect to Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "123456789"
graph = Graph(uri, auth=(username, password))

# Specify the output directory path
output_directory = r"C:\Users\rajas\Desktop\Execution Times\Neo4j"

#query functions
#gives first 50 books
def query_1(graph):
    start_time = time.time()
    result = graph.run("""
        MATCH (b:d250k_books)
        return b.book_author ;
    """)
    return round((time.time() - start_time) * 1000, 3)

#gives the details of book
def query_2(graph):
    start_time = time.time()
    result = graph.run("""
        MATCH (b:d250k_books)
        WHERE b.book_title = 'Book 374'
        RETURN b.book_title,b.book_author, b.book_isbn, b.borrow_date;
     """)
    return round((time.time() - start_time) * 1000, 3)

def query_3(graph):
    start_time = time.time()
    result = graph.run("""
        MATCH (book:Book)
        WHERE book.borrow_date IS NOT NULL AND book.return_date IS NULL
        RETURN book.book_title AS title, book.book_author AS author, book.book_isbn AS isbn;
        """)
    return round((time.time() - start_time) * 1000, 3)

#gives the number of books written by an author
def query_4(graph):
    start_time = time.time()
    result = graph.run("""
        MATCH (book:Book)
        RETURN book.book_author AS author, COUNT(book) AS bookCount
        ORDER BY bookCount DESC; 
    """)
    return round((time.time() - start_time) * 1000, 3)

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
            execution_time = query_func(graph)

            # Check for zero execution time
            if execution_time > 0:
                execution_times.append(execution_time)

        # Save execution times to a separate text file
        with open(output_file, mode='w') as file:
            file.write(f"{query_func.__name__} execution times: {execution_times}\n")

        print(f"{query_func.__name__} execution times saved to {output_file}")
