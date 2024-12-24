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
            "content": "You are a great graph creator. must create the graph based on data plot a chart. must Use matplotlib to plot graph. And then give matplotlib code. do not use plt.show in the end, if you get string in both x and y axis don't plot any graph",
        },
    ]
    messages.append({"role": "user", "content": user_query})
    if result:
        messages.append({"role": "assistant", "content": str(result)})
    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0,
        max_tokens=4096,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
    )
    print("Completion:", completion.choices[0].message.content)
    return (
        completion.choices[0].message.content
        if completion.choices
        else "Sorry, I couldn't generate a response."
    )


async def generate_graph_chart(response):
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
            except Exception as e:
                print(f"Error executing the code: {e}")
        else:
            print("No valid Python code block found in the response.")
    else:
        print(
            "The response does not contain any digits, indicating it may not be valid code."
        )

    return None
