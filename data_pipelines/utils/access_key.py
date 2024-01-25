from google.cloud import secretmanager
def get_secret_version(project: str, secret_id: str, version_id: str = "latest") -> str:
        """Get GPT API Key from Secrets Manager."""
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
