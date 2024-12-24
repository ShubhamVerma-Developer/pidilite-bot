import os
import requests
import base64
import pyodbc
import json
from decimal import Decimal
from botbuilder.core import ActivityHandler, TurnContext, MessageFactory
from botbuilder.schema import ChannelAccount, Activity, ActivityTypes, Attachment
import re
from config import DefaultConfig
from code.graph_plot import graph_agent, generate_graph_chart

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


def fetch_column_info(conn, table_name="primary_sales"):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}'"
    )
    return cursor.fetchall()


def select_table_for_nlp_query(nlp_query):
    tables = {
        "primary_sales": "The 'PrimarySales' table records sales data for various divisions, customers, and products. It includes:\nDivisionCode: Code representing the division.\nSalesGroupCode: Code for the sales group.\nCustomerCode: Unique code for the customer.\nPostingMonth: Month of the sales posting (e.g., 'January', 'Feb').\nMaterialCode: Code for the material or product.\nPrimarySalesReportingUnit: Unit in which primary sales are reported (numeric).\nPrimarySalesReportingValue: Value of the primary sales (numeric).\nPrimarySalesReportingUVG: Unit value growth of the primary sales (numeric, percentage).\nDivisionName: Name of the division.\nCustomerName: Name of the customer.\nCustomerGroup: Primary group classification of the customer.\nCustomerGroup1: Secondary group classification of the customer.\nCustomerGroup2: Tertiary group classification of the customer.\nCustomerGroup3: Quaternary group classification of the customer.\nCustomerTown: Town where the customer is located.\nCustomerZoneName: Zone name of the customer.\nCustomerNSMName: Name of the national sales manager for the customer.\nCustomerState: State where the customer is located.\nCustomerCountry: Country where the customer is located.\nSalesGroupName: Name of the sales group.\nMaterialDescription: Description of the material.\nMaterialFSNDescription: Description of the material's FSN (Fast, Slow, Non-moving) status.\nProductName: Name of the product.\nProductSubcategory: Subcategory of the product (Glue, Insulation Tape, Sealant etc).\nProductCategory: Category of the product (Household, Electrical etc.)\nCalendarDate: Date of the sales record (DD-MM-YYYY).\nCalendarMonthYear: Month and year of the calendar period (Month(In words)-YY, (e.g., 'Nov-24', 'Aug-21') ).\nFiscalYearQuarter: Fiscal year quarter in which the sales occurred (e.g., 'Q1', 'Q2').\nFiscalYear: Fiscal year of the sales record (YYYY).",
        "secondary_sales": "The 'SecondarySales' table records sales data from dealers to customers for various products. It includes:\nDealerKey: Unique identifier for the dealer.\nSalesGroupCode: Code for the sales group.\nDealerCode: Unique code for the dealer.\nCustomerCode: Unique code for the customer.\nMaterialCode: Code for the material or product.\nInvoiceMonth: Month of the invoice (e.g., 'January', 'Feb').\nSecondarySalesReportingUnit: Unit in which secondary sales are reported (numeric).\nSecondarySalesReportingValue: Value of the secondary sales (numeric).\nSecondarySalesReportingUVG: Unit value growth of the secondary sales (numeric, percentage).\nDealerName: Name of the dealer.\nDealerCustomerCode: Customer code associated with the dealer.\nDealerTSITerritoryCode: Territory code for the dealer's TSI (Territory Sales Incharge).\nDealerSalesmanType: Type of salesman assigned to the dealer (e.g., 'Field Sales', 'Online Sales').\nDealerSalesmanCode: Code identifying the salesman.\nDealerTSIKey: Key identifying the TSI for the dealer.\nDealerClass: Classification of the dealer (e.g., 'Group 1').\nDealerClassGroup: Group classification of the dealer.\nDealerType1: Primary type classification of the dealer.\nDealerType2: Secondary type classification of the dealer.\nDealerType3: Tertiary type classification of the dealer.\nDealerType4: Quaternary type classification of the dealer.\nDealerType5: Quinary type classification of the dealer.\nDealerAdoptedFlag: Flag indicating whether the dealer is adopted (Yes/No).\nDealerDisconnectedFlag: Flag indicating whether the dealer is disconnected (Yes/No).\nDealerActiveStatus: Active status of the dealer (e.g., 'Active', 'Inactive').\nDealerCluster: Cluster classification of the dealer (e.g., 'Cluster 1', 'Cluster 2').\nDealerActiveStatusTSICount: Count of active status TSIs associated with the dealer (numeric).",
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
    # Fetch columns info
    columns = fetch_column_info(conn, table_name)
    columns_str = ", ".join(
        [f'"{column[0]}" ({column[1]})' for column in columns]
    )  # Adding data types to column names

    # Fetch the table description
    tables = {
        "primary_sales": "The 'PrimarySales' table records sales data for various divisions, customers, and products. It includes:\nDivisionCode: Code representing the division.\nSalesGroupCode: Code for the sales group.\nCustomerCode: Unique code for the customer.\nPostingMonth: Month of the sales posting (e.g., 'January', 'Feb').\nMaterialCode: Code for the material or product.\nPrimarySalesReportingUnit: Unit in which primary sales are reported (numeric).\nPrimarySalesReportingValue: Value of the primary sales (numeric).\nPrimarySalesReportingUVG: Unit value growth of the primary sales (numeric, percentage).\nDivisionName: Name of the division.\nCustomerName: Name of the customer.\nCustomerGroup: Primary group classification of the customer.\nCustomerGroup1: Secondary group classification of the customer.\nCustomerGroup2: Tertiary group classification of the customer.\nCustomerGroup3: Quaternary group classification of the customer.\nCustomerTown: Town where the customer is located.\nCustomerZoneName: Zone name of the customer.\nCustomerNSMName: Name of the national sales manager for the customer.\nCustomerState: State where the customer is located.\nCustomerCountry: Country where the customer is located.\nSalesGroupName: Name of the sales group.\nMaterialDescription: Description of the material.\nMaterialFSNDescription: Description of the material's FSN (Fast, Slow, Non-moving) status.\nProductName: Name of the product.\nProductSubcategory: Subcategory of theproduct (Glue, Insulation Tape, Sealant etc).\nProductCategory: Category of the product (Household, Electrical etc.).\nCalendarDate: Date of the sales record (DD-MM-YYYY).\nCalendarMonthYear: Month and year of the calendar period (Month(In words)-YY, (e.g., 'Nov-24', 'Aug-21') ).\nFiscalYearQuarter: Fiscal year quarter in which the sales occurred (e.g., 'Q1', 'Q2').\nFiscalYear: Fiscal year of the sales record (YYYY).",
        "secondary_sales": "The 'SecondarySales' table records sales data from dealers to customers for various products. It includes:\nDealerKey: Unique identifier for the dealer.\nSalesGroupCode: Code for the sales group.\nDealerCode: Unique code for the dealer.\nCustomerCode: Unique code for the customer.\nMaterialCode: Code for the material or product.\nInvoiceMonth: Month of the invoice (e.g., 'January', 'Feb').\nSecondarySalesReportingUnit: Unit in which secondary sales are reported (numeric).\nSecondarySalesReportingValue: Value of the secondary sales (numeric).\nSecondarySalesReportingUVG: Unit value growth of the secondary sales (numeric, percentage).\nDealerName: Name of the dealer.\nDealerCustomerCode: Customer code associated with the dealer.\nDealerTSITerritoryCode: Territory code for the dealer's TSI (Territory Sales Incharge).\nDealerSalesmanType: Type of salesman assigned to the dealer (e.g., 'Field Sales', 'Online Sales').\nDealerSalesmanCode: Code identifying the salesman.\nDealerTSIKey: Key identifying the TSI for the dealer.\nDealerClass: Classification of the dealer (e.g., 'Group 1').\nDealerClassGroup: Group classification of the dealer.\nDealerType1: Primary type classification of the dealer.\nDealerType2: Secondary type classification of the dealer.\nDealerType3: Tertiary type classification of the dealer.\nDealerType4: Quaternary type classification of the dealer.\nDealerType5: Quinary type classification of the dealer.\nDealerAdoptedFlag: Flag indicating whether the dealer is adopted (Yes/No).\nDealerDisconnectedFlag: Flag indicating whether the dealer is disconnected (Yes/No).\nDealerActiveStatus: Active status of the dealer (e.g., 'Active', 'Inactive').\nDealerCluster: Cluster classification of the dealer (e.g., 'Cluster 1', 'Cluster 2').\nDealerActiveStatusTSICount: Count of active status TSIs associated with the dealer (numeric).",
    }

    table_description = tables.get(
        table_name, "No description available for this table."
    )

    # Construct the prompt message
    prompt_messages = [
        {
            "role": "system",
            "content": (
                f"Given the table '{table_name}' with columns {columns_str}, and the following description:\n"
                f"{table_description}\n"
                "Convert the following natural language query into an SQL query considering partial matches and relevant columns. "
                "Make sure the query should be compatible with Azure SQL Database. Ensure correct data type usage when comparing columns to values. "
                "Make sure the query should be very precise and accurate, it should not throw any error while executing to the database. "
                "It should only return the information asked in the NLP query. Do not return any additional information. "
                "Consider datatypes and column names accurately. Use CalendarDate (available in primary sales) in DD-MM-YYYY format while formatting SQL query. "
                "Consider CalendarMonthYear is in Month(In words)-YY (e.g., 'Nov-24', 'Aug-21')."
                "If the NLP query includes 'Jan, Feb, Mar' the SQL query should consider the full month name as 'January, February, and March'."
                "Use LIKE for partial matches and use TOP is based on the NLP query. "
                "Focus on columns that are likely targets based on the query's context."
            ),
        },
        {"role": "user", "content": nlp_query},
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
        return []
    except pyodbc.DataError as e:
        return []


def sql_to_nlp(sql_results):
    prompt_messages = [
        {
            "role": "system",
            "content": "Given the following SQL query results, generate a natural language response summarizing the data in a human-readable format. Consider the context of the original user's query. Do not include any currency signs in the response.",
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


async def create_image_chart_teams(chart_base64: str):
    image_attachment = Attachment(
        name="chart.png",
        content_type="image/png",
        content_url=f"data:image/png;base64,{chart_base64}",
    )

    return image_attachment


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
            print("------------------results---------------------" + str(results))
            if results:
                markdown_response = format_results_as_markdown(results)
                nlp_response = sql_to_nlp(
                    f"Question: {nlp_query}\nAnswer:\n{json.dumps(results, cls=DecimalEncoder)}"
                )

                combined_response = (
                    f"{markdown_response}\n\n\n\n**Summary**:\n{nlp_response}"
                )

                graph_response = await graph_agent(nlp_query, results)
                chart_base64 = await generate_graph_chart(graph_response)
                
                if chart_base64:  
                    image_attachment = await create_image_chart_teams(chart_base64)  
                    activity = Activity(  
                        type=ActivityTypes.message,  
                        text=combined_response,  
                        attachments=[image_attachment],  
                    )  
                else:  
                    activity = Activity(  
                        type=ActivityTypes.message,  
                        text=combined_response  
                    )  

                await turn_context.send_activity(activity)

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
