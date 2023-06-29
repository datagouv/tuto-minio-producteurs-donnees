from minio import Minio
from minio.error import S3Error
import time
from config import (
    MINIO_URL,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET
)


# Upload a file
def upload_file(bucket, path, file, minio_path):
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
            bucket, minio_path + file, path + file,
        )   
        print("--- Upload in %s seconds ---" % (time.time() - start_time))
        print('Ressource available : https://{}/{}/{}{}'.format(
            MINIO_URL,
            bucket,
            minio_path,
            file
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


# Exemple de chargement d'un fichier
try:
    upload_file(MINIO_BUCKET, './', 'test.csv', 'test/')
except S3Error as exc:
    print("error occurred.", exc)
