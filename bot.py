import os
import requests
import base64
import asyncio
import pyodbc
import json
import logging
from decimal import Decimal
from botbuilder.core import ActivityHandler, TurnContext, MessageFactory
from datetime import datetime, date, time
from botbuilder.schema import ChannelAccount, Activity, ActivityTypes, Attachment
from botbuilder.core import TurnContext, MessageFactory
from botbuilder.schema import (
    Activity,
    ActivityTypes,
    SuggestedActions,
    CardAction,
    ActionTypes,
)
from botbuilder.core import UserState, ConversationState, MemoryStorage
from botbuilder.core.bot_state import BotStatePropertyAccessor
import re
from config import DefaultConfig
from code.graph_plot import graph_agent, generate_graph_chart
from code.not_found_result import result_not_found

CONFIG = DefaultConfig()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

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


def nlp_to_sql(nlp_query, conn, table_names, user_email):
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
                "Always use 'FiscalYear' and 'CalendarMonth' in the generated sql query generation."
                "If the NLP query includes 'Jan, Feb, Mar' the SQL query should consider the full month name as 'January, February, and March'."
                "Use LIKE for partial matches and use TOP is based on the NLP query. "
                f"Include a filter to ensure that UserEmail = '{user_email}'."
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


async def sql_to_nlp(sql_results, original_nlp_query):
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


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode("utf-8")
        return super(CustomEncoder, self).default(obj)


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


async def create_image_chart_teams(chart_base64: str):
    image_attachment = Attachment(
        name="chart.png",
        content_type="image/png",
        content_url=f"data:image/png;base64,{chart_base64}",
    )

    return image_attachment


class MyBot(ActivityHandler):
    def __init__(self, user_state: UserState):
        self.user_email_accessor = user_state.create_property("UserEmail")
        self.user_login_status_accessor = user_state.create_property("LoginStatus")
        self.user_state = user_state

    async def on_message_activity(self, turn_context: TurnContext):
        user_email = await self.user_email_accessor.get(turn_context, None)
        login_status = await self.user_login_status_accessor.get(
            turn_context, "logged_out"
        )

        if login_status == "logged_out":
            if user_email is None:
                # If the user is not logged in and email is not provided, ask for email
                nlp_query = turn_context.activity.text
                if re.match(r"[^@]+@[^@]+\.[^@]+", nlp_query):
                    await self.user_email_accessor.set(turn_context, nlp_query)
                    await self.user_login_status_accessor.set(turn_context, "logged_in")
                    await self.user_state.save_changes(turn_context)
                    await turn_context.send_activity(
                        f"Thank you! Your email address {nlp_query} has been recorded. You are now logged in."
                    )
                else:
                    await turn_context.send_activity(
                        "Please enter your email address to log in:"
                    )
                return
            else:
                # If email is already stored, log in
                await self.user_login_status_accessor.set(turn_context, "logged_in")
                await self.user_state.save_changes(turn_context)
                await turn_context.send_activity("You are now logged in.")
                return

        # Handle login command
        if turn_context.activity.text.lower() == "login":
            if login_status == "logged_in":
                await turn_context.send_activity("You are already logged in.")
            else:
                await turn_context.send_activity(
                    "Please enter your email address to log in:"
                )
            return

        # Handle logout command
        if turn_context.activity.text.lower() == "logout":
            if login_status == "logged_out":
                await turn_context.send_activity("You are already logged out.")
            else:
                await self.user_login_status_accessor.set(turn_context, "logged_out")
                await self.user_email_accessor.delete(turn_context)
                await self.user_state.save_changes(turn_context)
                await turn_context.send_activity(
                    "You are now logged out. Please enter your email address to log in again."
                )
            return

        # If logged in, handle NLP query
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

        table_names = select_table_for_nlp_query(nlp_query, conn)
        sql_query = nlp_to_sql(nlp_query, conn, table_names, user_email)
        if sql_query:
            print("------------------sql_query---------------------" + sql_query)
            results = execute_sql_query(sql_query, conn)
            print("------------------results---------------------" + str(results))
            if results:
                markdown_response = format_results_as_markdown(results)

                # Run sql_to_nlp and graph_agent concurrently
                nlp_response, graph_response = await asyncio.gather(
                    sql_to_nlp(results, nlp_query),
                    graph_agent(nlp_query, results),
                )

                combined_response = (
                    f"{markdown_response}\n\n\n\n**Summary**:\n{nlp_response}"
                )
                chart_base64 = await generate_graph_chart(
                    nlp_query,
                    results,
                    graph_agent,
                )
                if chart_base64:
                    image_attachment = await create_image_chart_teams(chart_base64)
                    # Send the image attachment first
                    image_activity = Activity(
                        type=ActivityTypes.message, attachments=[image_attachment]
                    )
                    await turn_context.send_activity(image_activity)

                # Send the text message after the image
                text_activity = Activity(
                    type=ActivityTypes.message, text=combined_response
                )
                await turn_context.send_activity(text_activity)
            else:
                no_result_found = "Either you don't have access to the necessary data, or you might need to rephrase your query."
                await turn_context.send_activity(no_result_found)
        else:
            # no_result_found = "Either you don't have access to the necessary data, or you might need to rephrase your query."
            no_result_found = await result_not_found(nlp_query)
            await turn_context.send_activity(no_result_found)

        conn.close()
        print("Connection closed.")

    async def send_login_logout_buttons(self, turn_context: TurnContext):
        reply = MessageFactory.text("Choose an action:")
        reply.suggested_actions = SuggestedActions(
            actions=[
                CardAction(title="Login", type=ActionTypes.im_back, value="login"),
                CardAction(title="Logout", type=ActionTypes.im_back, value="logout"),
            ]
        )
        await turn_context.send_activity(reply)

    async def on_members_added_activity(
        self, members_added: ChannelAccount, turn_context: TurnContext
    ):
        for member_added in members_added:
            if member_added.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello and welcome!")


# Set up storage and state management
memory_storage = MemoryStorage()
user_state = UserState(memory_storage)
bot = MyBot(user_state)
