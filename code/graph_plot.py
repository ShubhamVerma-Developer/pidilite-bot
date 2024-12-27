import base64
import re
from openai import AzureOpenAI
from config import DefaultConfig
from io import BytesIO
import matplotlib.pyplot as plt

# Load configuration
CONFIG = DefaultConfig()
client = AzureOpenAI(
    azure_endpoint=CONFIG.GPT4V_SQL_TO_GRAPH_ENDPOINT,
    api_key=CONFIG.GPT4V_SQL_TO_GRAPH_KEY,
    api_version=CONFIG.GPT4V_SQL_TO_GRAPH_API_VERSION,
)


async def graph_agent(user_query, result):
    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert in generating graphs. Always create a graph based on the provided data and use the matplotlib library to plot the chart. "
                "Make sure your should always generate the accurate code so it can be executed without any error."
                "Strictly Make sure you defined all the necessary parameters correctly so it does not throw name error in the code."
                "Ensure to provide the complete matplotlib code. Avoid using plt.show() at the end."
                "If both the x-axis and y-axis contain string data, then convert it into numerical formate and then generate any graph. "
                "Ensure the data is processed accurately to facilitate successful graph creation every time."
                "Do not save the plot to a file."
                "In the response always contain digits, includes valide code block, and the code block should be in python."
            ),
        },
    ]
    messages.append({"role": "user", "content": user_query})
    if result:
        messages.append({"role": "assistant", "content": str(result)})
    completion = client.chat.completions.create(
        model=CONFIG.GPT4V_SQL_TO_GRAPH_MODEL_NAME,
        messages=messages,
        temperature=0.7,
        max_tokens=16384,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
    )
    return (
        completion.choices[0].message.content
        if completion.choices
        else "Sorry, I couldn't generate a response."
    )


async def generate_graph_chart(response):
    print("Generating graph chart from the response...", response)
    if any(char.isdigit() for char in response):
        code_pattern = re.compile(r"```python(.*?)```", re.DOTALL)
        code_match = code_pattern.search(response)
        executed = False

        if code_match:
            code = code_match.group(1).strip()
            try:
                exec_globals = {}
                exec(code, {"plt": plt}, exec_globals)
                executed = True

                buf = BytesIO()
                plt.savefig(buf, format="png")
                buf.seek(0)
                img_base64 = base64.b64encode(buf.read()).decode("utf-8")
                buf.close()
                plt.clf()
                return img_base64

            except SyntaxError as se:
                print(f"Syntax error in the code: {se}")
            except NameError as ne:
                print(f"Name error in the code: {ne}")
            except TypeError as te:
                print(f"Type error in the code: {te}")
            except Exception as e:
                print(f"Unexpected error executing the code: {e}")

        else:
            print("No valid Python code block found in the response.")

    else:
        print(
            "The response does not contain any digits, indicating it may not be valid code."
        )

    return None
