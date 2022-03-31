import logging
import json
import uuid
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import azure.functions as func
from azure.cosmos import cosmos_client,exceptions

keyVaultName = 'beta-app'
KVUri = f'https://{keyVaultName}.vault.azure.net'

credentials = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri,credential=credentials)

HOST=client.get_secret('DB-HOST').value
MASTER_KEY=client.get_secret('DB-MASTER-KEY').value
DATABASE_ID=client.get_secret('DB-ID').value
CONTAINER_ID=client.get_secret('DB-CONTAINER-ID').value

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Distinct Document Type HTTP trigger function processed a request.')

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

    query = 'SELECT distinct upper(c.document_type) FROM c order by c.document_type asc'
    
    distinct_document_type = list(
        container.query_items(
            query,
            enable_cross_partition_query=True
        )
    )

    if container:
        return func.HttpResponse(json.dumps(
            distinct_document_type
        ),mimetype='application/json',status_code=200)
    else:
        return func.HttpResponse(
             "Connection Error",
             status_code=400
        )
