# Amazon Books Data Engineering Pipeline

This project is a data engineering pipeline built using Apache Airflow. It scrapes book data from Amazon, processes it, and stores it in a PostgreSQL database.

## **Features**

- **Extract**: Scrapes book information (title, author, price, rating) from Amazon.
- **Transform**: Cleans and deduplicates the scraped data.
- **Load**: Stores the processed data in a PostgreSQL database.
- **Airflow DAG**: Manages and schedules tasks using Python and SQL operators.

## **Technologies**

- **Apache Airflow**: DAG orchestration.
- **Python**: Data extraction and processing.
- **PostgreSQL**: Data storage.
- **Docker**: Containerized development and deployment.

## **Project Structure**

```plaintext
DE PROJ AMAZON BOOKS
│
├── dags/
│   ├── amazon_books_dag.py          # DAG Python script
│
├── plugins/                         # For custom Airflow plugins (optional)
│
├── config/                          # Configuration files
│
├── logs/                            # Airflow log files
│
├── venv/                            # Virtual environment (add to .gitignore)
│
├── docker-compose.yaml              # Docker Compose file for Airflow and Postgres
│
├── .env                             # Environment variables
│
└── README.md                        # Documentation file
