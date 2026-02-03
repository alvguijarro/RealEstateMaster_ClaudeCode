
import sys
try:
    from google.genai import interactions
    print("SUCCESS: google.genai.interactions module found.")
    print("Attributes:")
    print(dir(interactions))
except ImportError:
    print("FAILURE: google.genai.interactions module NOT found.")

from google import genai
client = genai.Client(api_key="DUMMY")
print("\nClient dir:")
print([d for d in dir(client) if not d.startswith('_')])

print("\nClient.models dir:")
print([d for d in dir(client.models) if not d.startswith('_')])
