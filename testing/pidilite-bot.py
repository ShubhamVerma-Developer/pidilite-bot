import os
import requests
import base64
import pyodbc
import json
from decimal import Decimal

# Configuration
GPT4V_NLP_TO_SQL_KEY = ""
GPT4V_NLP_TO_SQL_ENDPOINT = ""

GPT4V_SQL_TO_NLP_KEY = ""
GPT4V_SQL_TO_NLP_ENDPOINT = ""

# Define your variables
SQL_SERVER = ""
SQL_DB = ""
SQL_USERNAME = ""
SQL_PWD = ""


# Function to establish connection
def establish_connection():
    connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{SQL_SERVER},1433;Database={SQL_DB};Uid={SQL_USERNAME};Pwd={SQL_PWD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return pyodbc.connect(connection_string)


def fetch_column_info(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}'"
    )
    return cursor.fetchall()


def select_table_for_nlp_query(nlp_query):
    tables = {
        "primary_updated_sales": "The 'PrimaryUpdatedSales' table records sales data for various divisions, customers, and products. It includes:\nDivisionCode: Code representing the division.\nSalesGroupCode: Code for the sales group.\nCustomerCode: Unique code for the customer.\nPostingMonth: Month of the sales posting (e.g., 'January', 'Feb').\nMaterialCode: Code for the material or product.\nPrimarySalesReportingUnit: Unit in which primary sales are reported (numeric).\nPrimarySalesReportingValue: Value of the primary sales (numeric).\nPrimarySalesReportingUVG: Unit value growth of the primary sales (numeric, percentage).\nDivisionName: Name of the division.\nCustomerName: Name of the customer.\nCustomerGroup: Primary group classification of the customer.\nCustomerGroup1: Secondary group classification of the customer.\nCustomerGroup2: Tertiary group classification of the customer.\nCustomerGroup3: Quaternary group classification of the customer.\nCustomerTown: Town where the customer is located.\nCustomerZoneName: Zone name of the customer.\nCustomerNSMName: Name of the national sales manager for the customer.\nCustomerState: State where the customer is located.\nCustomerCountry: Country where the customer is located.\nSalesGroupName: Name of the sales group.\nMaterialDescription: Description of the material.\nMaterialFSNDescription: Description of the material's FSN (Fast, Slow, Non-moving) status.\nProductName: Name of the product.\nProductSubcategory: Subcategory of the product (Glue, Insulation Tape, Sealant etc).\nProductCategory: Category of the product (Household, Electrical etc.)\nCalendarDate: Date of the sales record (DD-MM-YYYY).\nCalendarMonthYear: Month and year of the calendar period (Month(In words)-YY, (e.g., 'Nov-24', 'Aug-21') ).\nFiscalYearQuarter: Fiscal year quarter in which the sales occurred (e.g., 'Q1', 'Q2').\nFiscalYear: Fiscal year of the sales record (YYYY).\nUserEmail: Email of the user associated with the record.\nCalendarMonth: Month of the calendar period (January, February etc.).",
        "secondary_updated_sales": "The 'SecondaryUpdatedSales' table records sales data from dealers to customers for various products. It includes:\nDealerKey: Unique identifier for the dealer.\nSalesGroupCode: Code for the sales group.\nDealerCode: Unique code for the dealer.\nCustomerCode: Unique code for the customer.\nMaterialCode: Code for the material or product.\nInvoiceMonth: Month of the invoice (e.g., 'January', 'Feb').\nSecondarySalesReportingUnit: Unit in which secondary sales are reported (numeric).\nSecondarySalesReportingValue: Value of the secondary sales (numeric).\nSecondarySalesReportingUVG: Unit value growth of the secondary sales (numeric, percentage).\nDealerName: Name of the dealer.\nDealerCustomerCode: Customer code associated with the dealer.\nDealerTSITerritoryCode: Territory code for the dealer's TSI (Territory Sales Incharge).\nDealerSalesmanType: Type of salesman assigned to the dealer (e.g., 'Field Sales', 'Online Sales').\nDealerSalesmanCode: Code identifying the salesman.\nDealerTSIKey: Key identifying the TSI for the dealer.\nDealerClass: Classification of the dealer (e.g., 'Group 1').\nDealerClassGroup: Group classification of the dealer.\nDealerType1: Primary type classification of the dealer.\nDealerType2: Secondary type classification of the dealer.\nDealerType3: Tertiary type classification of the dealer.\nDealerType4: Quaternary type classification of the dealer.\nDealerType5: Quinary type classification of the dealer.\nDealerAdoptedFlag: Flag indicating whether the dealer is adopted (Yes/No).\nDealerDisconnectedFlag: Flag indicating whether the dealer is disconnected (Yes/No).\nDealerActiveStatus: Active status of the dealer (e.g., 'Active', 'Inactive').\nDealerCluster: Cluster classification of the dealer (e.g., 'Cluster 1', 'Cluster 2').\nDealerActiveStatusTSICount: Count of active status TSIs associated with the dealer (numeric).\nUserEmail: Email of the user associated with the record.",
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
    response.raise_for_status()

    content = response.json()["choices"][0]["message"]["content"].strip()
    table_name = content.split(":")[1].strip() if ":" in content else content
    return table_name


def nlp_to_sql(nlp_query, conn, table_name, user_email):
    columns = fetch_column_info(conn, table_name)
    columns_str = ", ".join([f'"{column[0]}" ({column[1]})' for column in columns])

    tables = {
        "primary_updated_sales": "The 'PrimaryUpdatedSales' table records sales data for various divisions, customers, and products. It includes:\nDivisionCode: Code representing the division.\nSalesGroupCode: Code for the sales group.\nCustomerCode: Unique code for the customer.\nPostingMonth: Month of the sales posting (e.g., 'January', 'Feb').\nMaterialCode: Code for the material or product.\nPrimarySalesReportingUnit: Unit in which primary sales are reported (numeric).\nPrimarySalesReportingValue: Value of the primary sales (numeric).\nPrimarySalesReportingUVG: Unit value growth of the primary sales (numeric, percentage).\nDivisionName: Name of the division.\nCustomerName: Name of the customer.\nCustomerGroup: Primary group classification of the customer.\nCustomerGroup1: Secondary group classification of the customer.\nCustomerGroup2: Tertiary group classification of the customer.\nCustomerGroup3: Quaternary group classification of the customer.\nCustomerTown: Town where the customer is located.\nCustomerZoneName: Zone name of the customer.\nCustomerNSMName: Name of the national sales manager for the customer.\nCustomerState: State where the customer is located.\nCustomerCountry: Country where the customer is located.\nSalesGroupName: Name of the sales group.\nMaterialDescription: Description of the material.\nMaterialFSNDescription: Description of the material's FSN (Fast, Slow, Non-moving) status.\nProductName: Name of the product.\nProductSubcategory: Subcategory of the product (Glue, Insulation Tape, Sealant etc).\nProductCategory: Category of the product (Household, Electrical etc.)\nCalendarDate: Date of the sales record (DD-MM-YYYY).\nCalendarMonthYear: Month and year of the calendar period (Month(In words)-YY, (e.g., 'Nov-24', 'Aug-21') ).\nFiscalYearQuarter: Fiscal year quarter in which the sales occurred (e.g., 'Q1', 'Q2').\nFiscalYear: Fiscal year of the sales record (YYYY).\nUserEmail: Email of the user associated with the record.\nCalendarMonth: Month of the calendar period (January, February etc.).",
        "secondary_updated_sales": "The 'SecondaryUpdatedSales' table records sales data from dealers to customers for various products. It includes:\nDealerKey: Unique identifier for the dealer.\nSalesGroupCode: Code for the sales group.\nDealerCode: Unique code for the dealer.\nCustomerCode: Unique code for the customer.\nMaterialCode: Code for the material or product.\nInvoiceMonth: Month of the invoice (e.g., 'January', 'Feb').\nSecondarySalesReportingUnit: Unit in which secondary sales are reported (numeric).\nSecondarySalesReportingValue: Value of the secondary sales (numeric).\nSecondarySalesReportingUVG: Unit value growth of the secondary sales (numeric, percentage).\nDealerName: Name of the dealer.\nDealerCustomerCode: Customer code associated with the dealer.\nDealerTSITerritoryCode: Territory code for the dealer's TSI (Territory Sales Incharge).\nDealerSalesmanType: Type of salesman assigned to the dealer (e.g., 'Field Sales', 'Online Sales').\nDealerSalesmanCode: Code identifying the salesman.\nDealerTSIKey: Key identifying the TSI for the dealer.\nDealerClass: Classification of the dealer (e.g., 'Group 1').\nDealerClassGroup: Group classification of the dealer.\nDealerType1: Primary type classification of the dealer.\nDealerType2: Secondary type classification of the dealer.\nDealerType3: Tertiary type classification of the dealer.\nDealerType4: Quaternary type classification of the dealer.\nDealerType5: Quinary type classification of the dealer.\nDealerAdoptedFlag: Flag indicating whether the dealer is adopted (Yes/No).\nDealerDisconnectedFlag: Flag indicating whether the dealer is disconnected (Yes/No).\nDealerActiveStatus: Active status of the dealer (e.g., 'Active', 'Inactive').\nDealerCluster: Cluster classification of the dealer (e.g., 'Cluster 1', 'Cluster 2').\nDealerActiveStatusTSICount: Count of active status TSIs associated with the dealer (numeric).\nUserEmail: Email of the user associated with the record.",
    }

    table_description = tables.get(
        table_name, "No description available for this table."
    )

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
                "Consider CalendarMonthYear is in Month(In words)-YY (e.g., 'Nov-24', 'Aug-21') and CalendarMonth has values like January, February etc."
                "Always consider 'FiscalYear' and 'CalendarMonth' in the query."
                "If the NLP query includes 'Jan, Feb, Mar' the SQL query should consider the full month name as 'January, February, and March'."
                "Use LIKE for partial matches and use TOP is based on the NLP query. "
                "Focus on columns that are likely targets based on the query's context."
                "If you are including CustomerName, SalesGroupName then include the columns CustomerCode, SalesGroupCode in the SELECT statement."
                f"Include a filter to ensure that UserEmail = '{user_email}'."
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
    response.raise_for_status()

    content = response.json()["choices"][0]["message"]["content"].strip()
    sql_query = (
        content.split("```sql")[1].split("```")[0].strip()
        if "```sql" in content
        else ""
    )
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


def sql_to_nlp(sql_results, original_nlp_query):
    prompt_messages = [
        {
            "role": "system",
            "content": "Given the following SQL query results, generate a natural language response summarizing the data in a human-readable format. Consider the context of the original user's query. Do not include any currency signs in the response.",
        },
        {
            "role": "user",
            "content": f"Original Query: {original_nlp_query}\nSQL Results: {json.dumps(sql_results, cls=DecimalEncoder)}",
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
        "max_tokens": 4096,
    }

    response = requests.post(GPT4V_SQL_TO_NLP_ENDPOINT, headers=headers, json=payload)
    response.raise_for_status()

    content = response.json()["choices"][0]["message"]["content"].strip()
    return content


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)


if __name__ == "__main__":
    conn = establish_connection()
    user_email = None

    while True:
        if not user_email:
            user_email = input(
                "Please enter your email address (or type 'exit' to quit): "
            )
            if user_email.lower() == "exit":
                conn.close()
                break

        if user_email.lower() == "logout":
            user_email = None
            continue

        # Check if the email is associated with any records
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM primary_updated_sales WHERE UserEmail = '{user_email}'"
        )
        primary_count = cursor.fetchone()[0]
        cursor.execute(
            f"SELECT COUNT(*) FROM secondary_updated_sales WHERE UserEmail = '{user_email}'"
        )
        secondary_count = cursor.fetchone()[0]

        if primary_count == 0 and secondary_count == 0:
            print("You don't have access to any data.")
            user_email = None
            continue

        nlp_query = input("Please enter your query in natural language: ")

        # Print the NLP query
        print(
            "------------------------------------------------------------------------------------------------------------"
        )
        print(f"NLP Query: {nlp_query}")

        # Determine the table name based on the NLP query
        table_name = select_table_for_nlp_query(nlp_query)
        print(
            "------------------------------------------------------------------------------------------------------------"
        )
        print(f"Selected Table: {table_name}")

        # Generate SQL query based on the selected table and NLP query
        sql_query = nlp_to_sql(nlp_query, conn, table_name, user_email)
        print(
            "------------------------------------------------------------------------------------------------------------"
        )
        print(f"Generated SQL Query: {sql_query}")

        if sql_query:
            results = execute_sql_query(sql_query, conn)
            if results:
                nlp_response = sql_to_nlp(results, nlp_query)
                print(
                    "------------------------------------------------------------------------------------------------------------"
                )
                print(f"NLP Response from SQL Query Results: {nlp_response}")
                print(
                    "------------------------------------------------------------------------------------------------------------"
                )
            else:
                print("No results found or you don't have access to this data.")
        else:
            print(
                "Failed to generate SQL query. Please refine your natural language query."
            )

    print("Connection closed.")
