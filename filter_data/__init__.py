from datetime import datetime
import os
import json
import logging
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import azure.functions as func
from azure.cosmos import cosmos_client, exceptions


keyVaultName = 'beta-app'
KVUri = f'https://{keyVaultName}.vault.azure.net'

credentials = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri,credential=credentials)

# AZURE_STORAGE_CONNECTION_STRING=client.get_secret('BLOB-STORAGE-CONNECTION-STRING').value
# AZURE_STORAGE_CONTAINER_NAME=client.get_secret('AZ-BLOB-STORAGE-CONTAINER-NAME').value
HOST=client.get_secret('DB-HOST').value
MASTER_KEY=client.get_secret('DB-MASTER-KEY').value
DATABASE_ID=client.get_secret('DB-ID').value
CONTAINER_ID=client.get_secret('DB-CONTAINER-ID').value

def filter_data(container,param):
    # data = {
    #     '_id':str
    #     'file_extension':str
    #     'file_size':Number
    #     'file_name':str
    #     'document_type':str
    #     'reviewed_by':str
    #     'uploaded_by':str
    #     'is_tagged':boolean
    #     'location':str
    #     'sender':str
    #     'created_at':datetime
    #     'updated_at':datetime
    #     'signed_at':datetime
    #     'is_signed':boolean
    #     'blob_url':str
    # }
    q = 'select * from c where'
    
    for k in param:
        if k=='from_date':
            continue
        if k=='to_date':
            continue
        if k=='digi_signed': 
            q+=f' c.{k}={param.get(k)} and'
        if k=='location':
            sub_query = str([f"{item}" for item in param.get(k).split(',')]).replace('[','').replace(']','')
            q+=f' c.{k} in ({sub_query}) and'
        if k=='document_type':
            sub_query = str([f"{item}" for item in param.get(k).split(',')]).replace('[','').replace(']','')
            q+=f' c.{k} in ({sub_query}) and'

    if ('from_date' in param) and ('to_date' in param): 
        q+= f" (c.created_at between '{param.get('from_date')}' and '{param.get('to_date')}') and"
    elif ('from_date' in param) and (not 'to_date' in param): 
        q+= f" c.created_at >= '{param.get('from_date')}' and"
    elif (not 'from_date' in param) and ('to_date' in param): 
        q+= f" c.created_at <= '{param.get('to_date')}' and"

    q = q[:len(q)-3]

    if 'sort_by' in param and 'sort_type' in param: q+=f' order by {param.get("sort_by")} {param.get("sort_type")}'
    elif not 'sort_by' in param and 'sort_type' in param: q+=f' order by c.created_at {param.get("sort_type")}'
    elif 'sort_by' in param and not 'sort_type' in param: q+=f' order by {param.get("sort_by")} desc'
    else: q+=f' order by c.created_at desc'

    print(f'query is :')
    logging.error(q)
    print('--'*10)
    items = list(container.query_items(
        query = q,
        enable_cross_partition_query=True
    ))

    print(items)

    return items


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
            filter_data(container,req.get_json())
        ),mimetype='application/json',status_code=200)
    else:
        return func.HttpResponse(
             "Connection Error",
             status_code=400
        )