from py2neo import Graph
from pathlib import Path

uri = "bolt://localhost:7687"
username = "neo4j"
password = "123456789"

graph = Graph(uri, auth=(username, password))

database_name = 'library'

uri_with_db = f"{uri}/db/{database_name}"
graph_db = Graph(uri_with_db, auth=(username, password))

csv_files_and_labels = [
    (r'C:\datasets\250k_books.csv', 'd250k_books'),
    (r'C:\datasets\500k_books.csv', 'd500k_books'),
    (r'C:\datasets\750k_books.csv', 'd750k_books'),
    (r'C:\datasets\1million_books.csv', 'd1million_books')
]

for csv_file_path, label in csv_files_and_labels:
    csv_file_path = Path(csv_file_path)  # Convert to Path for better path manipulation

    # Used the `as_uri()` method to get a URI representation of the path
    cypher_query = f"""
    LOAD CSV WITH HEADERS FROM '{csv_file_path.as_uri()}' AS row
    CREATE (:Book:{label} {{
        book_title: row.book_title,
        book_author: row.book_author,
        book_isbn: row.book_isbn,
        borrower_name: row.borrower_name,
        borrower_email: row.borrower_email,
        borrower_phone: row.borrower_phone,
        borrow_date: date(row.borrow_date),
        return_date: date(row.return_date)
    }})
    """

    graph_db.run(cypher_query)
    print(f"Data from {csv_file_path} inserted successfully.")

print("Data inserted into 'library' database.")
