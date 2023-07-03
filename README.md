# Project Summary

This project is part of the DAT108U course and involves building an ETL (Extract, Transform, Load) pipeline for processing log and song data and loading it into a PostgreSQL database. The ETL process extracts data from JSON files, transforms it to the required format, and loads it into several tables representing the schema of the Sparkify music streaming app.

The ETL pipeline consists of a Python script (`etl.py`) that connects to the Sparkify database, processes the log and song data, and performs the necessary data transformations. The script utilizes the psycopg2 library to interact with the PostgreSQL database and the pandas library for data manipulation.

The project also includes SQL queries defined in `sql_queries.py` for creating the necessary tables, inserting data into the tables, and performing data retrieval operations.

## Project Structure

The project repository contains the following files:

- `etl.py`: The main Python script that performs the ETL process by extracting and processing log and song data, and loading it into the Sparkify database.
- `sql_queries.py`: Contains the SQL queries used by the ETL script to create tables, insert data, and perform data retrieval operations.
- `song and log files`: data files under the data folder
- `create_tables.py`: Removes any existing tables and creates the database and the tables 
- `etl.ipynb`: is a step-by-step implementation of the ETL for a single song file and a single log file - for testing purposes. 
- `test.ipynb`: Sanity check for the results. Checks if data has been put into the tables. Run this after `etl.py`
- Other necessary project files, such as additional files or scripts required for running the project.

## Getting Started

To run the ETL pipeline and set up the Sparkify database, follow these steps:

1. Set up a PostgreSQL database and obtain the connection details (host, port, database name, username, password).
2. Clone this repository to your local machine.
3. Install the required dependencies by running `pip install -r requirements.txt`.
4. Modify the database connection details in the `etl.py` script to match your PostgreSQL database.
5. Run the `etl.py` script to start the ETL process: `python etl.py`.
6. Monitor the console for any error messages or logs generated during the ETL process.

## Dependencies

The project requires the following dependencies:

- Python 3.x
- psycopg2 (Python library for PostgreSQL database connectivity)
- pandas (Python library for data manipulation)

Make sure to install these dependencies before running the project.

## Further Details

For more detailed information about the project, including data schema, ETL process details, and SQL queries, refer to the project documentation or consult the source code.

