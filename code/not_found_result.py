from openai import AzureOpenAI
from config import DefaultConfig

# Load configuration
CONFIG = DefaultConfig()
client = AzureOpenAI(
    azure_endpoint=CONFIG.GPT4V_SQL_TO_GRAPH_ENDPOINT,
    api_key=CONFIG.GPT4V_SQL_TO_GRAPH_KEY,
    api_version=CONFIG.GPT4V_SQL_TO_GRAPH_API_VERSION,
)


async def result_not_found(user_query):
    messages = [
        {
            "role": "system",
            "content": (
                "If the user's input is a greeting or contains details not found, "
                "respond with a short and simple message. "
                "For example, 'Hello! How can I help you today?' or 'I'm sorry, I couldn't find any results for that.'"
            ),
        },
    ]
    messages.append({"role": "user", "content": user_query})

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
        print(f"OpenAI request failed: {e}")
        return "Sorry, I couldn't generate a response."
