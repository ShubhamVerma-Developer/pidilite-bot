import os
import requests
import base64
import pyodbc
import json
import logging
from decimal import Decimal
from datetime import datetime, date, time
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

GPT4V_NLP_TO_SQL_KEY = ""
GPT4V_NLP_TO_SQL_ENDPOINT = ""

GPT4V_SQL_TO_NLP_KEY = ""
GPT4V_SQL_TO_NLP_ENDPOINT = ""

# Define your variables
SQL_SERVER = ""
SQL_DB = ""
SQL_USERNAME = ""
SQL_PWD = ""


def establish_connection():
    try:
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{SQL_SERVER},1433;"
            f"Database={SQL_DB};"
            f"Uid={SQL_USERNAME};"
            f"Pwd={SQL_PWD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        conn = pyodbc.connect(connection_string)
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to establish database connection: {e}")
        raise


def fetch_table_info(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
        )
        tables = cursor.fetchall()

        table_info = {}
        for (table_name,) in tables:
            cursor.execute(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}'"
            )
            columns = cursor.fetchall()
            column_info = "| Column Name | Data Type |\n|-------------|-----------|\n"
            column_info += "\n".join(
                [f"| {column[0]} | {column[1]} |" for column in columns]
            )

            cursor.execute(f"SELECT TOP 2 * FROM {table_name}")
            rows = cursor.fetchall()
            sample_data = [
                dict(zip([column[0] for column in cursor.description], row))
                for row in rows
            ]

            sample_data_markdown = (
                "| "
                + " | ".join(
                    cursor.description[i][0] for i in range(len(cursor.description))
                )
                + " |\n"
            )
            sample_data_markdown += (
                "| " + " | ".join("---" for _ in cursor.description) + " |\n"
            )
            sample_data_markdown += "\n".join(
                [
                    "| " + " | ".join(str(value) for value in row.values()) + " |"
                    for row in sample_data
                ]
            )

            table_info[table_name] = {
                "columns": column_info,
                "sample_data": sample_data_markdown,
            }
        logging.info("Fetched table information successfully.")
        return table_info
    except Exception as e:
        logging.error(f"Failed to fetch table information: {e}")
        raise


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode("utf-8")
        return super(CustomEncoder, self).default(obj)


def select_table_for_nlp_query(nlp_query, conn):
    table_info = fetch_table_info(conn)

    markdown_table_info = "\n\n".join(
        [
            f"### Table: {table_name}\n\n{info['columns']}\n\nSample Data:\n{info['sample_data']}"
            for table_name, info in table_info.items()
        ]
    )

    prompt_messages = [
        {
            "role": "system",
            "content": (
                "Given the following table descriptions, column data types, and sample data in Markdown format, select the most appropriate table(s) for the given natural language query. "
                "Provide only the table names, separated by commas, in your response. Do not include any explanations or additional text."
            ),
        },
        {
            "role": "user",
            "content": f"Table descriptions:\n\n{markdown_table_info}\n\nNLP Query: {nlp_query}",
        },
    ]

    headers = {
        "Content-Type": "application/json",
        "api-key": GPT4V_NLP_TO_SQL_KEY,
    }

    payload = {
        "messages": prompt_messages,
        "temperature": 0.3,
        "top_p": 0.95,
        "max_tokens": 4096,
    }

    try:
        response = requests.post(
            GPT4V_NLP_TO_SQL_ENDPOINT, headers=headers, json=payload
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"].strip()
        table_names = [name.strip() for name in content.split(",") if name.strip()]

        # Validate table names against the actual schema
        valid_table_names = set(table_info.keys())
        selected_tables = [table for table in table_names if table in valid_table_names]

        logging.info(f"Selected tables for NLP query: {selected_tables}")
        return selected_tables
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during API request: {e}")
        raise


def nlp_to_sql(nlp_query, conn, table_names):
    table_info = fetch_table_info(conn)

    descriptions = "\n".join(
        [
            f"### Table: {table_name}\n\nColumns:\n{table_info[table_name]['columns']}\n\nSample Data:\n{table_info[table_name]['sample_data']}"
            for table_name in table_names
        ]
    )

    prompt_messages = [
        {
            "role": "system",
            "content": (
                f"Given the tables {', '.join(table_names)} with the following descriptions and columns:\n"
                f"{descriptions}\n"
                "Convert the following natural language query into an SQL query using JOINs if necessary."
                "Only return the information requested in the query. Do not include any additional columns or data."
                "Ensure the query is precise, accurate, and compatible with Azure SQL Database."
                "Consider current year is 2025, but we have data till 2024 so you should consider current year as 2024."
                "Generate single query only in response. If join is done make sure to use proper join conditions."
                "Consider data types and column names and sample raw values and generate sql query accurately. Use JOINs to combine tables as needed."
            ),
        },
        {"role": "user", "content": nlp_query},
    ]
    print(prompt_messages)

    headers = {
        "Content-Type": "application/json",
        "api-key": GPT4V_NLP_TO_SQL_KEY,
    }

    payload = {
        "messages": prompt_messages,
        "temperature": 0.7,
        "top_p": 0.95,
        "max_tokens": 4096,
    }

    try:
        response = requests.post(
            GPT4V_NLP_TO_SQL_ENDPOINT, headers=headers, json=payload
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"].strip()
        if "```sql" in content and "```" in content.split("```sql")[1]:
            sql_query = content.split("```sql")[1].split("```")[0].strip()
        else:
            sql_query = ""
        logging.info(f"Generated SQL query: {sql_query}")
        return sql_query
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during API request: {e}")
        raise


def execute_sql_query(sql_query, conn):
    cursor = conn.cursor()
    adjusted_sql_query = sql_query.replace("CURRENT_DATE", "CAST(GETDATE() AS DATE)")

    try:
        cursor.execute(adjusted_sql_query)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logging.info("Executed SQL query successfully.")
        return results
    except pyodbc.ProgrammingError as e:
        logging.error(f"SQL Programming Error: {e}")
        return []
    except pyodbc.DataError as e:
        logging.error(f"SQL Data Error: {e}")
        return []


def sql_to_nlp(sql_results, original_nlp_query):
    prompt_messages = [
        {
            "role": "system",
            "content": (
                "Given the following SQL query results, generate a natural language response summarizing the data in a human-readable format. "
                "Consider the context of the original user's query. Do not include any currency signs in the response."
            ),
        },
        {
            "role": "user",
            "content": f"Original Query: {original_nlp_query}\nSQL Results: {json.dumps(sql_results, cls=CustomEncoder)}",
        },
    ]

    headers = {
        "Content-Type": "application/json",
        "api-key": GPT4V_SQL_TO_NLP_KEY,
    }

    payload = {
        "messages": prompt_messages,
        "temperature": 0.7,
        "top_p": 0.95,
        "max_tokens": 16384,
    }

    try:
        response = requests.post(
            GPT4V_SQL_TO_NLP_ENDPOINT, headers=headers, json=payload
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"].strip()
        logging.info("Converted SQL results to NLP response successfully.")
        return content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during API request: {e}")
        return (
            "An error occurred while processing the SQL results into natural language."
        )


def process_query(nlp_query, conn):
    try:
        table_names = select_table_for_nlp_query(nlp_query, conn)
        if not table_names:
            logging.warning("No tables selected for the query.")
            return "No relevant tables found for your query."

        sql_query = nlp_to_sql(nlp_query, conn, table_names)
        if not sql_query:
            logging.warning("Failed to generate SQL query.")
            return "Failed to generate SQL query. Please refine your natural language query."

        logging.info(f"Generated SQL Query: {sql_query}")

        results = execute_sql_query(sql_query, conn)
        if not results:
            logging.info("No results found for the SQL query.")
            return "No results found."

        nlp_response = sql_to_nlp(results, nlp_query)
        return nlp_response
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return "An error occurred while processing your query."


if __name__ == "__main__":
    conn = establish_connection()

    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            nlp_query = input(
                "Please enter your query in natural language (or type 'exit' to quit): "
            )
            if nlp_query.lower() == "exit":
                conn.close()
                break

            future = executor.submit(process_query, nlp_query, conn)
            nlp_response = future.result()
            print(
                "------------------------------------------------------------------------------------------------------------"
            )
            print(f"NLP Response: {nlp_response}")
            print(
                "------------------------------------------------------------------------------------------------------------"
            )

    logging.info("Connection closed.")
