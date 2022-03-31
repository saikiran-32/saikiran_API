import logging
import os
import json
import azure.functions as func
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

keyVaultName = 'beta-app'
KVUri = f'https://{keyVaultName}.vault.azure.net'

credentials = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri,credential=credentials)

HOST=client.get_secret('DB-HOST').value
MASTER_KEY=client.get_secret('DB-MASTER-KEY').value
DATABASE_ID=client.get_secret('DB-ID').value
CONTAINER_ID=client.get_secret('DB-CONTAINER-ID').value

def get_all_data(container):
    vendor_data = list(
            container.read_all_items()
        )
    print('Retrived all vendor data..')
    return vendor_data

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart",
                                    user_agent_overwrite=True)
    try:
        db = client.get_database_client(DATABASE_ID)
        print('Database with id \'{0}\' was found'.format(DATABASE_ID))

    except exceptions.CosmosResourceExistsError:
        print('Database with id \'{0}\' not found'.format(DATABASE_ID))

    # setup container for this sample
    try:
        container = db.get_container_client(CONTAINER_ID)
        print('Container with id \'{0}\' was found'.format(CONTAINER_ID))

    except exceptions.CosmosResourceExistsError:
        print('Container with id \'{0}\' not found'.format(CONTAINER_ID))

    if container:
        return func.HttpResponse(json.dumps(
            get_all_data(container)
        ),mimetype='application/json',status_code=200)
    else:
        return func.HttpResponse(
             "Connection Error",
             status_code=400
        )
