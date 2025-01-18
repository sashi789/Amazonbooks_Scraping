
#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time

#1) fetch amazon data (extract) 2) clean data (transform)

import random

# user_agents = [
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36",
#     "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
# ]

# headers = {
#     "Referer": "https://www.amazon.com/",
#     "Sec-Ch-Ua": "Not_A Brand",
#     "Sec-Ch-Ua-Mobile": "?0",
#     "Sec-Ch-Ua-Platform": "macOS",
#     "User-agent": random.choice(user_agents),
# }

# Inside your loop



def get_amazon_data_books(num_books, ti):
    import random
    import time
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    ]

    headers = {
        "Referer": "https://www.amazon.com/",
        "Sec-Ch-Ua": "Not_A Brand",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "macOS",
        "User-agent": random.choice(user_agents),
    }

    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 503])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    base_url = f"https://www.amazon.com/s?k=data+engineering+books"
    books = []
    seen_titles = set()
    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        print(f"Fetching URL: {url}")

        try:
            response = session.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                book_containers = soup.find_all("div", {"class": "s-result-item"})
                if not book_containers:
                    print(f"No book containers found on page {page}")
                    break

                for book in book_containers:
                    title = book.find("span", {"class": "a-text-normal"})
                    author = book.find("a", {"class": "a-size-base"})
                    price = book.find("span", {"class": "a-price-whole"})
                    rating = book.find("span", {"class": "a-icon-alt"})

                    if title and author and price and rating:
                        book_title = title.text.strip()
                        if book_title not in seen_titles:
                            seen_titles.add(book_title)
                            books.append({
                                "Title": book_title,
                                "Author": author.text.strip(),
                                "Price": price.text.strip(),
                                "Rating": rating.text.strip(),
                            })
                            print(f"Fetched book: {book_title}")

                page += 1
                time.sleep(random.uniform(2, 5))  # Add random delay
            else:
                print(f"Failed to fetch page {page}, status code: {response.status_code}")
                if "captcha" in response.text.lower():
                    print("CAPTCHA detected. Stopping requests.")
                    break
                break
        except Exception as e:
            print(f"Error fetching data from Amazon: {e}")
            break

    print(f"Total books fetched: {len(books)}")
    ti.xcom_push(key='book_data', value=books)



#3) create and store data in table on postgres (load)
    
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books-connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books-connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task
