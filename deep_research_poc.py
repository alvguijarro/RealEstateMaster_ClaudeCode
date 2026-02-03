
import os
from google import genai
from google.genai import types

# Load Google API Key
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

def test_deep_research():
    print("Testing Deep Research Pro (Interactions API)...")
    if not GOOGLE_API_KEY:
        print("Error: GOOGLE_API_KEY not set.")
        return

    client = genai.Client(api_key=GOOGLE_API_KEY)
    
    prompt = "Investiga sobre la situación del mercado inmobiliario en el barrio de Salamanca, Madrid."
    
    try:
        print(f"Sending prompt to 'gemini-2.0-flash' (baseline test)...")
        # Testing with 'input' instead of 'messages'
        response = client.interactions.create(
            model='gemini-2.0-flash',
            input=[
                {'role': 'user', 'parts': [{'text': prompt}]}
            ]
        )
        print("\n[SUCCESS] Baseline Interactions call worked!")
        # print(response) # Reduce output

        print(f"\nNow trying 'deep-research-pro-preview-12-2025' with agent param...")
        # Use 'agent' for deep-research-pro
        response_pro = client.interactions.create(
            agent='deep-research-pro-preview-12-2025',
            input=[
                {'role': 'user', 'parts': [{'text': prompt}]}
            ]
        )
        print("\n[SUCCESS] Deep Research Pro call worked!")
        print(response_pro)
        
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\n[ERROR]: {e}")
        if hasattr(e, 'status_code'):
             print(f"Status Code: {e.status_code}")
        if hasattr(e, 'body'):
             print(f"Body: {e.body}")
        
        # Fallback inspection if parameter error
        import inspect
        try:
            print("\nSignature of create:")
            print(inspect.signature(client.interactions.create))
        except:
            pass

if __name__ == "__main__":
    test_deep_research()
