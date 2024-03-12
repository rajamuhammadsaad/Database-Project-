import os
import csv
from faker import Faker


fake = Faker()

fields = ['book_title', 'book_author', 'book_isbn', 'borrower_name', 'borrower_email', 'borrower_phone', 'borrow_date', 'return_date']

directory = 'C:\\datasets\\'
csv_file_path = '1million_books.csv'
full_csv_file_path = os.path.join(directory, csv_file_path)

if not os.path.exists(directory):
    os.makedirs(directory)

if not os.path.exists(full_csv_file_path):
    with open(full_csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(fields)

def generate_book_data(book_title):
    book_author = fake.name()
    book_isbn = fake.isbn13()
    borrower_name = fake.name()
    borrower_email = fake.email()
    borrower_phone = fake.phone_number()
    borrow_date = fake.date_this_decade()
    return_date = fake.date_between(start_date=borrow_date, end_date='+30d')
    return [book_title, book_author, book_isbn, borrower_name, borrower_email, borrower_phone, borrow_date, return_date]

num_records_1million = 1000000

with open(full_csv_file_path, 'w', newline='') as csvfile_1million:
    writer_1million = csv.writer(csvfile_1million)
    writer_1million.writerow(fields)
    for book_id in range(1, num_records_1million + 1):
        data = generate_book_data(f"Book {book_id}")
        writer_1million.writerow(data)

csv_data = []
with open(full_csv_file_path, 'r') as csvfile_1million:
    reader = csv.reader(csvfile_1million)
    next(reader)
    csv_data = list(reader)

data_750k = csv_data[:750000]
data_500k = csv_data[:500000]
data_250k = csv_data[:250000]

csv_file_750k = os.path.join(directory, '750k_books.csv')
csv_file_500k = os.path.join(directory, '500k_books.csv')
csv_file_250k = os.path.join(directory, '250k_books.csv')

def write_csv_data(file_name, data):
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(fields)
        writer.writerows(data)

write_csv_data(csv_file_750k, data_750k)
write_csv_data(csv_file_500k, data_500k)
write_csv_data(csv_file_250k, data_250k)

print("Fake book data generation and splitting are done.")