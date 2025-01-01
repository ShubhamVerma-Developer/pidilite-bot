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


async def graph_agent(user_query, result, max_retries=3):
    for attempt in range(max_retries):
        messages = [
            {
                "role": "system",
                "content": (
                    "You are an expert in generating graphs. Always create a graph based on the provided data and use the matplotlib library to plot the chart. "
                    "Make sure your should always generate the accurate code so it can be executed without any error."
                    "Strictly Make sure you defined all the necessary parameters correctly so it does not throw name error in the code."
                    "Ensure to provide the complete matplotlib code. Avoid using plt.show() at the end."
                    "Make sure when you generate bar plot, all parameters are defined correctly like bar_width, xlabel and ylabel etc."
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

        try:
            completion = client.chat.completions.create(
                model=CONFIG.GPT4V_SQL_TO_GRAPH_MODEL_NAME,
                messages=messages,
                temperature=0.5,
                max_tokens=16384,
                top_p=0.95,
                frequency_penalty=0,
                presence_penalty=0,
                stop=None,
            )
            response = (
                completion.choices[0].message.content
                if completion.choices
                else "Sorry, I couldn't generate a response."
            )
            return response
        except Exception as e:
            print(f"OpenAI request failed on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise e


async def generate_graph_chart(user_query, result, graph_agent_func, max_retries=3):
    retries = 0
    response = await graph_agent_func(user_query, result)  # Initial response
    while retries < max_retries:
        print(f"Generating graph chart from the response... Attempt {retries + 1}")
        if any(char.isdigit() for char in response):
            code_pattern = re.compile(r"```python(.*?)```", re.DOTALL)
            code_match = code_pattern.search(response)
            if code_match:
                code = code_match.group(1).strip()
                try:
                    exec_globals = {}
                    exec(code, {"plt": plt}, exec_globals)

                    buf = BytesIO()
                    plt.savefig(buf, format="png")
                    buf.seek(0)
                    img_base64 = base64.b64encode(buf.read()).decode("utf-8")
                    buf.close()
                    plt.clf()
                    return img_base64

                except NameError as ne:
                    print(f"Name error in the code: {ne}")
                    # Request new graph code if NameError occurs
                    response = await graph_agent_func(user_query, result)
                    retries += 1
                    continue
                except (SyntaxError, TypeError, Exception) as e:
                    print(f"Error in graph generation attempt {retries + 1}: {e}")
                    break  # Do not retry on other errors
            else:
                print("No valid Python code block found in the response.")
                break
        else:
            print(
                "The response does not contain any digits, indicating it may not be valid code."
            )
            break
    return None
