from google.cloud import secretmanager

PROJECT_ID = "project-d8285645-02d2-4be2-a85"

def get_secret(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")