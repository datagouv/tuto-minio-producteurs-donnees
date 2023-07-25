from minio import Minio
from minio.error import S3Error
import os
import requests
import time
from config import (
    MINIO_URL,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    DATAGOUV_URL,
    DATAGOUV_TOKEN,
    DATASET_ID,
    RESOURCE_ID,
)


# Upload a file
def upload_file_to_minio(bucket, local_path, local_file_name, minio_path, minio_file_name):
    start_time = time.time()
    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=True
    )
    # check if bucket exists.
    found = client.bucket_exists(bucket)
    if found:
        print("Bucket {} exists".format(bucket))   
        client.fput_object(
            bucket, minio_path + minio_file_name, local_path + local_file_name,
        )   
        print("--- Upload in %s seconds ---" % (time.time() - start_time))
        print('Ressource available : https://{}/{}/{}{}'.format(
            MINIO_URL,
            bucket,
            minio_path,
            minio_file_name
        ))


# List all files from a bucket
def list_files(bucket):
    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=True
    )
    # check if bucket if exist.
    found = client.bucket_exists(bucket)
    if found:
        print("Bucket {} exists".format(bucket))
        objects = client.list_objects(bucket, recursive=True)
        for obj in objects:
            print(obj.object_name)
            print(obj.etag)


# Reference file to data.gouv.fr
def post_remote_resource(
    api_key: str,
    dataset_id: str,
    title: str,
    format: str,
    remote_url: str,
    filesize: int,
    type: str = "main",
    schema: dict = {},
    description: str = "",
    resource_id: str = None,
):
    """Create a post in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        dataset_id (str): id of the dataset
        title (str): resource title
        format (str): resource format
        remote_url (str): resource distant URL
        filesize (int): resource size (bytes)
        type (str): type of resource
        schema (str): schema of the resource (if relevant)
        description (str): resource description
        resource_id (str): resource id (if modifying an existing resource)

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    headers = {
        "X-API-KEY": api_key,
    }
    payload = {
        'title': title,
        'description': description,
        'url': remote_url,
        'type': type,
        'filetype': 'remote',
        'format': format,
        'schema': schema,
        'filesize': filesize,
    }
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
        print(f"Putting '{title}' at {url}")
        r = requests.put(
            url,
            json=payload,
            headers=headers
        )
        print(f"See {DATAGOUV_URL}/fr/datasets/{dataset_id}/")
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/"
        print(f"Posting '{title}' at {url}")
        r = requests.post(
            url,
            json=payload,
            headers=headers
        )
    r.raise_for_status()
    return r.json()


# Exemple de chargement d'un fichier
try:
    minio_path = "test/"
    local_path = './'
    local_file_name = 'test.csv'
    minio_file_name = 'test-uploaded.csv'
    file_size = os.path.getsize(local_path + local_file_name)

    upload_file_to_minio(MINIO_BUCKET, local_path, local_file_name, minio_path, minio_file_name)

    post_remote_resource(
        api_key=DATAGOUV_TOKEN,
        dataset_id=DATASET_ID,
        title="Mon super titre",
        format="csv",
        remote_url=f"https://{MINIO_URL}/{MINIO_BUCKET}/{minio_path}{minio_file_name}",
        filesize=str(file_size),
        type="main",
        schema={},
        description="Cette super ressource est issu de ma super base de données et est structurée de X colonnes de types Y",
        resource_id=RESOURCE_ID,
    )
except S3Error as exc:
    print("error occurred.", exc)
