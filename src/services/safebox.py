from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import os
 
class Safebox():
    @staticmethod
    def get_secret(key):
        try:
            vaulturi = f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/"
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vaulturi, credential=credential)
            return client.get_secret(key).value
        except:
            print("Chave não encontrada: ", key)
            return None