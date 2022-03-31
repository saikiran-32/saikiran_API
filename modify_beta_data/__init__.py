from datetime import datetime
import logging
import os
import json
import azure.functions as func
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST')
MASTER_KEY = os.getenv('MASTER_KEY')
DATABASE_ID = os.getenv('DATABASE_ID')
CONTAINER_ID = os.getenv('CONTAINER_ID')

keyVaultName = 'beta-app'
KVUri = f'https://{keyVaultName}.vault.azure.net'

credentials = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri,credential=credentials)

HOST=client.get_secret('DB-HOST').value
MASTER_KEY=client.get_secret('DB-MASTER-KEY').value
DATABASE_ID=client.get_secret('DB-ID').value
CONTAINER_ID=client.get_secret('DB-CONTAINER-ID').value

def modify_record(container,param):
    item_id = param.get('id')
    try:
        item = container.read_item(item_id, partition_key=item_id)
    except exceptions.CosmosResourceNotFoundError as e:
        print(e)
        return json.dumps({
            'error':'Item not found',
            'message':'Item is not present in the database..'
        })
    for k in param:
        item[k] = param.get(k)
    item['updated_at'] = datetime.now().isoformat()
    print(item)

    try:
        res = container.upsert_item(item)
    except exceptions.CosmosResourceNotFoundError as rnf:
        print('\n Resouce not found {rnf}\n')
        return json.dumps({
            'error':'Resource Not Found',
            'message':'No record found for particular ID'
        })
    return res

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Modify Beta trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        pass

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
        return func.HttpResponse(json.dumps(modify_record(container,req_body)),mimetype='application/json',status_code=200)
    else:
        return func.HttpResponse(
             "This Modify Beta triggered function executed successfully. Pass a data in the request body for a personalized response.",
             status_code=404
        )
