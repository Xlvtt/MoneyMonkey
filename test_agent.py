import os
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv("/Users/nalyapin/Desktop/Projects/HSE/MoneyMonkey/moneymonkey/.env")

client = OpenAI(
    base_url="https://router.huggingface.co/v1",
    api_key=os.environ["HF_TOKEN"],
)

completion = client.chat.completions.create(
    model="openai/gpt-oss-120b:fastest",
    messages=[
        {
            "role": "user",
            "content": "How many 'G's in 'huggingface'?"
        }
    ],
)
print(completion.choices[0].message.content)