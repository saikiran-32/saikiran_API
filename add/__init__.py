from datetime import datetime
import logging
import json
import uuid
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import azure.functions as func
from azure.cosmos import cosmos_client,exceptions
import get_db_container
import get_blob_container
import exception_handler

keyVaultName = 'beta-app'
KVUri = f'https://{keyVaultName}.vault.azure.net'

credentials = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri,credential=credentials)

HOST=client.get_secret('DB-HOST').value
MASTER_KEY=client.get_secret('DB-MASTER-KEY').value
DATABASE_ID=client.get_secret('DB-ID').value
CONTAINER_ID=client.get_secret('DB-CONTAINER-ID').value

AZURE_STORAGE_CONTAINER_NAME=client.get_secret('AZ-BLOB-STORAGE-CONTAINER-NAME').value

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP add beta data trigger function processed a request.')

    try:
        data = req.files.get('files')
        form_data = req.form['form_data']
    except ValueError as ve:
        return func.HttpResponse(
            {
                "error":str(ve),
                "message": str(ve.args)
            }, status_code=400
        )
    
    # First store data to blob stroage and then provide the blob url to form_data to store it in the database
    # At this moment we are just taking one file at a time
    try:
        blob_url = store_to_blob(data)
    except Exception as e:
        exception_handler.exception_handler(__file__,409,e)
        return func.HttpResponse(
            json.dumps({
                "error":str(e),
                "message": str(e.args)
            }), status_code=409
        )
    
    form_data = json.loads(form_data)
    form_data.update({"id":str(uuid.uuid4())})
    form_data.update({'blob_url':blob_url})
    
    try:
        container = get_db_container.get_container(__file__,DATABASE_ID,CONTAINER_ID)
        store_to_db(req,container,form_data)
    except Exception as e:
        exception_handler.exception_handler(__file__,404,e)
        return func.HttpResponse(
            json.dumps({
                "error":str(e),
                "message":str(e.args)
            }), status_code=404
        )

    logging.info('All success...')
    return func.HttpResponse(
        json.dumps({
            "error":False,
            "message":"Upload Success"
        }), status_code= 200
    )
    
def store_to_blob(payload):
    return get_blob_container.upload_to_blob_storage(__file__,AZURE_STORAGE_CONTAINER_NAME,payload)

def store_to_db(req,container,form_data):
    preprocessed_data = preprocess_data(req,form_data)
    container.create_item(body=preprocessed_data)

def preprocess_data(req,form_data):
    logging.error(dir(req))
    logging.error(form_data)
    logging.error(dir(req.files))
    files = req.files.get('files')
    data = {
        'id':form_data.get('id'),
        'file_extension':files.filename.split('.')[-1],
        'file_size':files.content_length,
        'file_name':files.filename,
        'document_type':None,
        'document_date':None,
        'digi_signed':None,
        'location':form_data.get('location',None),
        'sender':form_data.get('sender',"System"),
        'created_at':datetime.now().isoformat(),
        'reviewed':None,
        'updated_at':None,
        'signed_at':None,
        'blob_url':form_data.get('blob_url'),
        'thumbnail':'https://jd24testblobstorage.blob.core.windows.net/thumbnails/833px-PDF_file_icon.svg.png'
    }

    return data

#testing