
import os
from google import genai
import inspect

api_key = os.getenv('GOOGLE_API_KEY', 'DUMMY')
client = genai.Client(api_key=api_key)

print("Client attributes:")
for name in dir(client):
    if not name.startswith('_'):
        print(f"- {name}")

print("\nChecking for 'interactions' or similar subsystems:")
if hasattr(client, 'interactions'):
    print("Found 'interactions' attribute!")
    print(dir(client.interactions))
elif hasattr(client, 'aio'):
    print("Checking async client attributes:")
    for name in dir(client.aio):
        if not name.startswith('_'):
            print(f"  - {name}")

print("\nChecking types for Interaction related classes:")
from google.genai import types
for name in dir(types):
    if 'Interaction' in name:
        print(f"- {name}")
