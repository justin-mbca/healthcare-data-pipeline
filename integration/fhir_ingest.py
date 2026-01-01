# Example: FHIR API Ingestion
import requests

def fetch_fhir_resource(url, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()
