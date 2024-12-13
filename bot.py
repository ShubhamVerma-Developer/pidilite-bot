import os
import requests
import base64
import pyodbc
import json
from decimal import Decimal
from botbuilder.core import ActivityHandler, TurnContext
from botbuilder.schema import ChannelAccount, Activity, ActivityTypes
import re
from config import DefaultConfig

CONFIG = DefaultConfig()

# Configuration
GPT4V_NLP_TO_SQL_KEY = CONFIG.GPT4V_NLP_TO_SQL_KEY
GPT4V_NLP_TO_SQL_ENDPOINT = CONFIG.GPT4V_NLP_TO_SQL_ENDPOINT

GPT4V_SQL_TO_NLP_KEY = CONFIG.GPT4V_SQL_TO_NLP_KEY
GPT4V_SQL_TO_NLP_ENDPOINT = CONFIG.GPT4V_SQL_TO_NLP_ENDPOINT

# Define your variables
SQL_SERVER = CONFIG.SQL_SERVER
SQL_DB = CONFIG.SQL_DB
SQL_USERNAME = CONFIG.SQL_USERNAME
SQL_PWD = CONFIG.SQL_PWD


def establish_connection():
    try:
        print("Establishing connection...")
        connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{SQL_SERVER},1433;Database={SQL_DB};Uid={SQL_USERNAME};Pwd={SQL_PWD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=180;"

        conn = pyodbc.connect(connection_string)
        print("Connection established.")
        return conn

    except pyodbc.InterfaceError as e:
        print(f"Connection failed due to interface error: {e}")
    except pyodbc.DatabaseError as e:
        print(f"Database error occurred: {e}")
    except pyodbc.Error as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Connection attempt finished.")


def fetch_column_info(conn, table_name="salesdata"):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}'"
    )
    return cursor.fetchall()


def select_table_for_nlp_query(nlp_query):
    tables = {
        "Inventory": "The 'Inventory' table records inventory data for 'atQor Technologies Private Limited' in INR. It includes:\nCompanyName: Company name.\nMonthYear: Date (DD/MM/YY).\nCurrency: Currency (INR).\nInventoryType: Type of inventory (Raw Material, WIP, Finished Goods).\nOpening: Opening balance.\nIn: Inventory added.\nOut: Inventory removed.\nClosing: Closing balance.",
        "BankBalance": "The 'BankBalance' table records financial data for 'atQor Technologies Private Limited' in INR. It includes:\nCompanyName: Company name.\nMonthStartDate: Start date (DD/MM/YY).\nCurrency: Currency (INR).\nBankName: Name of the bank.\nOpeningBalance: Opening balance in the account.\nBalance: Current balance.\nTotalBankLimit: Total credit limit provided by the bank.\nLimitUtilized: Amount of the limit utilized.\nAvailableLimit: Remaining available limit.",
        "TradeReceiveable": "The 'TradeReceiveable' table records accounts receivable data for 'atQor Technologies Private Limited' in INR. It includes:\nCompanyName: Company name.\nCustomerName: Name of the customer.\nCustomerType: Type of customer (e.g., InterCompany, Others).\nReportingDate: Reporting date (DD/MM/YY).\nInvoiceDate: Date of the invoice (DD/MM/YY).\nCurrency: Currency (INR).\nTotalReceivables: Total amount receivable.\nNotInvoiced (Yes/No): Indicates if not invoiced.\nAging Buckets:\n0-90: Receivables due within 0-90 days.\n91-120: Receivables due within 91-120 days.\n121-180: Receivables due within 121-180 days.\n181-270: Receivables due within 181-270 days.\n271-360: Receivables due within 271-360 days.\n360: Receivables overdue by more than 360 days.\nTotalDue>90: Total amount due over 90 days.\nBadDebt: Amount considered as bad debt.\nLegal: Amount under legal proceedings.\nRetention: Retention amount.",
        "TradePayable": "The 'TradePayable' table records accounts payable data for 'atQor Technologies Private Limited' in INR. It includes:\nCompanyName: Company name.\nVendorName: Name of the vendor.\nVendorType: Type of vendor (e.g., InterCompany, Others).\nReportingDate: Reporting date (DD/MM/YY).\nCurrency: Currency (INR).\nTotalPayables: Total amount payable.\nPendingInvoices: Number of pending invoices.\nAging Buckets:\n0-90: Payables due within 0-90 days.\n91-120: Payables due within 91-120 days.\n121-180: Payables due within 121-180 days.\n181-270: Payables due within 181-270 days.\n271-360: Payables due within 271-360 days.\n360: Payables overdue by more than 360 days.\nTotalPayables<180: Total amount payable within 180 days.\nCostOfOverdue: Cost associated with overdue payables.",
    }

    prompt_messages = [
        {
            "role": "system",
            "content": "Given the following table descriptions, select the most appropriate table for the given natural language query. Provide me the table name only in the response.",
        },
        {
            "role": "user",
            "content": f"Table descriptions: {json.dumps(tables)}\nNLP Query: {nlp_query}",
        },
    ]

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

    response = requests.post(GPT4V_NLP_TO_SQL_ENDPOINT, headers=headers, json=payload)
    response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code

    content = response.json()["choices"][0]["message"]["content"].strip()
    table_name = content.split(":")[1].strip() if ":" in content else content
    return table_name


def nlp_to_sql(nlp_query, conn, table_name):
    columns = fetch_column_info(conn, table_name)
    columns_str = ", ".join(
        [f'"{column[0]}" ({column[1]})' for column in columns]
    )  # Adding data types to column names
    prompt_messages = [
        {
            "role": "system",
            "content": f"Given the table '{table_name}' with columns {columns_str}, convert the following natural language query into an SQL query considering partial matches and relevant columns. Make sure the query should be compatible with Azure SQL Database. Ensure correct data type usage when comparing columns to values. Make sure the query should be very precise and accurate, it should not throw any error while executing to the database. It should only return the information asked in the NLP query. Do not return any additional information. Consider datatypes and column names accurately. The CompanyName value will always be 'atQor Technologies Private Limited'. If another name appears in the NLP query, select the column related to that other name. This does not mean that the you include CompanyName as 'atQor Technologies Private Limited' in all the sql query. Use dates, month, and year in 'DD/MM/YY' format always in the SQL query. If the date is not provided, take the 1st of that month and keep the year as 2024. If the query is based on a month or year, handle the query accordingly based on the date. For example, if only the month is given, assume the 1st of that month in 2024; if only the year is given, assume the 1st of January 2024. BOB stand for Bank of Baroda use 'Bank of Baroda' in the sql query. User 'Raw Material' always even user wrote 'Raw Materials' or anything else. Date should be in DD/MM/YY format in sql query always. Do not use CompanyName until it is specified in the user query. User 'interCompany' in the query always. Use LIKE for partial matches and use TOP is based on the nlp query. Focus on columns that are likely targets based on the query's context.",
        },
        {"role": "user", "content": nlp_query},
    ]
    # print(prompt_messages)

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

    response = requests.post(GPT4V_NLP_TO_SQL_ENDPOINT, headers=headers, json=payload)
    response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code

    content = response.json()["choices"][0]["message"]["content"].strip()
    if "```sql" in content and "```" in content.split("```sql")[1]:
        sql_query = content.split("```sql")[1].split("```")[0].strip()
    else:
        sql_query = ""  # Fallback if parsing fails
    return sql_query


def execute_sql_query(sql_query, conn):
    cursor = conn.cursor()
    adjusted_sql_query = sql_query.replace("CURRENT_DATE", "CAST(GETDATE() AS DATE)")

    try:
        cursor.execute(adjusted_sql_query)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return results
    except pyodbc.ProgrammingError as e:
        print(f"Error executing query: {e}")
        return []
    except pyodbc.DataError as e:
        print(f"Data error executing query: {e}")
        return []


def sql_to_nlp(sql_results):
    prompt_messages = [
        {
            "role": "system",
            "content": "You are a ERP Agent Bot. Given the following SQL query results, generate a natural language response summarizing the data in a human-readable format. Also, do not use a dollar sign when there are amounts. Consider the context of the original user's query. Do not include any currency signs in the response. If you find the greeting pattern in the query, simply provide a short and pin point reply to the greeting.",
        },
        {"role": "user", "content": json.dumps(sql_results, cls=DecimalEncoder)},
    ]

    headers = {
        "Content-Type": "application/json",
        "api-key": GPT4V_SQL_TO_NLP_KEY,
    }

    payload = {
        "messages": prompt_messages,
        "temperature": 0.7,
        "top_p": 0.95,
        "max_tokens": 4096,
    }

    try:
        response = requests.post(
            GPT4V_SQL_TO_NLP_ENDPOINT, headers=headers, json=payload
        )
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    except requests.RequestException as e:
        raise SystemExit(f"Failed to make the request. Error: {e}")

    content = response.json()["choices"][0]["message"]["content"].strip()

    return content


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)  # or float(obj) if you prefer
        return super(DecimalEncoder, self).default(obj)


def format_results_as_markdown(results):
    if not results:
        return "No results found."

    # Generate Markdown table header
    headers = results[0].keys()
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

    # Generate Markdown table rows
    rows = []
    for result in results:
        row = (
            "| " + " | ".join(str(result.get(header, "")) for header in headers) + " |"
        )
        rows.append(row)

    # Combine header, separator, and rows
    markdown_table = "\n".join([header_row, separator_row] + rows)

    return f"{markdown_table}"


class MyBot(ActivityHandler):
    async def on_message_activity(self, turn_context: TurnContext):
        conn = establish_connection()

        nlp_query = turn_context.activity.text

        # Send typing activity to show that the bot is processing the request
        typing_activity = Activity(type=ActivityTypes.typing)
        await turn_context.send_activity(typing_activity)

        # Check for greetings and respond
        greetings_pattern = re.compile(
            r"^(hi|hii|hello|hey|hee|hola|howdy|greetings|hi there|good morning|good afternoon|good evening|sup|yo|what\'s up|morning|afternoon|evening|salutations|bonjour|namaste|what\'s good|how\'s it going|hiya|ahoy|aloha|shalom|ciao|hey there|hello there|peace|wassup|how are you|how are you doing|how do you do|hey ya|hey you|hi everyone|hi all)$",
            re.IGNORECASE,
        )

        if greetings_pattern.match(nlp_query.strip()):
            await turn_context.send_activity("Hello, how can I assist you!")
            return

        table_name = select_table_for_nlp_query(nlp_query)

        sql_query = nlp_to_sql(nlp_query, conn, table_name)
        if sql_query:
            print("------------------sql_query---------------------" + sql_query)
            results = execute_sql_query(sql_query, conn)
            if results:
                markdown_response = format_results_as_markdown(results)
                nlp_response = sql_to_nlp(
                    f"Question: {nlp_query}\nAnswer:\n{json.dumps(results, cls=DecimalEncoder)}"
                )

                combined_response = (
                    f"{markdown_response}\n\n\n\n**Summary**:\n{nlp_response}"
                )

                await turn_context.send_activity(combined_response)
            else:
                no_result_found = sql_to_nlp(
                    f"Question: {nlp_query}\nAnswer:\nNo answer found."
                )
                await turn_context.send_activity(no_result_found)
        else:
            no_result_found = sql_to_nlp(
                f"Question: {nlp_query}\nAnswer:\nI'm not sure I understand. Can you give more details or rephrase?"
            )
            await turn_context.send_activity(no_result_found)

        conn.close()
        print("Connection closed.")

    async def on_members_added_activity(
        self, members_added: ChannelAccount, turn_context: TurnContext
    ):
        for member_added in members_added:
            if member_added.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello and welcome!")
