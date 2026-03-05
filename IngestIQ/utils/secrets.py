"""
IngestIQ Secrets Manager - Multi-Cloud
"""
import json

class SecretsManager:
    def __init__(self, cloud_provider="aws", region="ap-south-1"):
        self.cloud_provider = cloud_provider.lower()
        self.region = region
        self._init_client()
    
    def _init_client(self):
        """Initialize cloud-specific secrets client"""
        if self.cloud_provider == "aws":
            import boto3
            self.client = boto3.client('secretsmanager', region_name=self.region)
        elif self.cloud_provider == "azure":
            from azure.keyvault.secrets import SecretClient
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            vault_url = f"https://default-keyvault.vault.azure.net/"
            self.client = SecretClient(vault_url=vault_url, credential=credential)
        elif self.cloud_provider == "gcp":
            from google.cloud import secretmanager
            self.client = secretmanager.SecretManagerServiceClient()
        else:
            raise ValueError(f"Unsupported cloud provider: {self.cloud_provider}")
    
    def get_secret(self, secret_name):
        """Get secret from cloud provider"""
        try:
            if self.cloud_provider == "aws":
                response = self.client.get_secret_value(SecretId=secret_name)
                return json.loads(response['SecretString'])
            
            elif self.cloud_provider == "azure":
                secret = self.client.get_secret(secret_name)
                return json.loads(secret.value)
            
            elif self.cloud_provider == "gcp":
                name = f"projects/default-project/secrets/{secret_name}/versions/latest"
                response = self.client.access_secret_version(request={"name": name})
                return json.loads(response.payload.data.decode("UTF-8"))
                
        except Exception as e:
            raise Exception(f"Failed to get secret {secret_name}: {str(e)}")