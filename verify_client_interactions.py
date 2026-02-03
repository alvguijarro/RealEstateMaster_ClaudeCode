
import os
from google import genai

client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))

print(f"Has interactions? {hasattr(client, 'interactions')}")

if hasattr(client, 'interactions'):
    print("Interactions methods:")
    print([d for d in dir(client.interactions) if not d.startswith('_')])
else:
    print("Client has no 'interactions' attribute. Trying to import interactions directly.")
    from google.genai import interactions
    # Maybe we need to pass client? 
    # Usually SDKs have client.interactions.
