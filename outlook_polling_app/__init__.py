import datetime
import logging
from imap_tools import *
import azure.functions as func
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
from uuid import uuid4
import os

load_dotenv()

USER = os.getenv('MAILUSER')
PASSWORD = os.getenv('PASSWORD')
MAILSERVER_ADDRESS = os.getenv('MAIL_SERVER')

keyVaultName = 'beta-app'
KVUri = f'https://{keyVaultName}.vault.azure.net'

credentials = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri,credential=credentials)

AZURE_STORAGE_CONNECTION_STRING=client.get_secret('BLOB-STORAGE-CONNECTION-STRING').value
AZURE_STORAGE_CONTAINER_NAME=client.get_secret('AZ-BLOB-STORAGE-CONTAINER-NAME').value
HOST=client.get_secret('DB-HOST').value
MASTER_KEY=client.get_secret('DB-MASTER-KEY').value
DATABASE_ID=client.get_secret('DB-ID').value
CONTAINER_ID=client.get_secret('DB-CONTAINER-ID').value


def get_blob_service_client():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    except:
        return False,None
    return True,blob_service_client

def upload_to_blob(blob_service_client,payload,filename):
    # Create a blob client using the attachement file name as the name for the blob
    try:
        blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE_CONTAINER_NAME, blob=filename)
        print(f'\nUploading to Azure Storage as blob:\n\t {filename}')
        blob_client.upload_blob(payload)
    except:
        logging.error(f'Error in uploading file: {filename}')
        return False,None
    
    logging.info(f'{filename} upload success....')
    return True,blob_client.url

def get_db_client():
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
    try:
        # setup database for this sample
        try:
            db = client.create_database(id=DATABASE_ID)
            print('Database with id \'{0}\' created'.format(DATABASE_ID))

        except exceptions.CosmosResourceExistsError:
            db = client.get_database_client(DATABASE_ID)
            # print('Database with id \'{0}\' was found'.format(DATABASE_ID))

        # setup container for this sample
        try:
            container = db.create_container(id=CONTAINER_ID, partition_key=PartitionKey(path='/partitionKey'))
            print('Container with id \'{0}\' created'.format(CONTAINER_ID))

        except exceptions.CosmosResourceExistsError:
            container = db.get_container_client(CONTAINER_ID)
            # print('Container with id \'{0}\' was found'.format(CONTAINER_ID))

        return True,container
    except exceptions.CosmosHttpResponseError as e:
        print('\ncaught an error. {0}'.format(e.message))
        return False,None

def create_data_from_email_polling(container,payload,file_url,sender):
    logging.info('\n Creating data in db...\n')
    
    data = {
        'id':str(uuid4()),
        'file_extension':payload.filename.split('.')[-1],
        'file_size':payload.size,
        'file_name':payload.filename,
        'document_type':None,
        'document_date':None,
        'digi_signed':None,
        'location':None,
        'sender':sender if sender else 'system',
        'created_at':datetime.now().isoformat(),
        'reviewed':None,
        'updated_at':None,
        'signed_at':None,
        'blob_url':file_url,
        'thumbnail':'https://jd24testblobstorage.blob.core.windows.net/thumbnails/833px-PDF_file_icon.svg.png'
    }

    container.create_item(body=data)
    logging.info('\n Created successfully...\n')



def connect( username:str, password:str, connection_string:str):
    try:
        client = MailBox(connection_string)
        client.login(username, password)
    except Exception as e:
        return False,None
    return True,client

def save_unseen_mail_attachment(client:MailBox,db_container):
    for msg in client.fetch(AND(seen=False,date=datetime.date.today()),mark_seen=False):
        print(msg.flags)
        print(msg.from_)
        print(msg.subject)

        # check for attachment in msg and download it
        for att in msg.attachments:
            if att.filename.endswith('.jpg') or att.filename.endswith('.png') or att.filename.endswith('.pdf') or att.filename.endswith('.jpeg'):
                print(att.filename)
                ret, bs_client = get_blob_service_client()
                if ret:
                    resp, blob_url = upload_to_blob(bs_client,att.payload,att.filename)
                    if resp:
                        create_data_from_email_polling(db_container,att,blob_url,msg.from_)
        client.flag(msg.uid,MailMessageFlags.SEEN, True)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    print(HOST,MASTER_KEY,DATABASE_ID,CONTAINER_ID)

    logging.info('Trying to logging into the mail server')
    resp, clt = connect(USER,PASSWORD,MAILSERVER_ADDRESS)
    if resp:
        logging.info('Login Success....')
        logging.info('fetching unseen mails for attachment...')
        ret,db_container = get_db_client()
        if ret: save_unseen_mail_attachment(clt,db_container)
        logging.info('fetching done...')
        res = clt.logout()
        print(res)
        logging.info('Logged out of the mail server')
    else:
        logging.info('Server not reachable')