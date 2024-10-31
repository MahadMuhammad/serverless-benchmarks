import io
import os
import uuid

import boto3


class storage:
    instance = None
    client = None

    def __init__(self):
        self.client = boto3.client('s3')

    @staticmethod
    def unique_name(name):
        name, extension = os.path.splitext(name)
        return '{name}.{random}{extension}'.format(
                    name=name,
                    extension=extension,
                    random=str(uuid.uuid4()).split('-')[0]
                )

    def upload(self, bucket, file, filepath, overwrite=False):
        key_name = storage.unique_name(file)
        if (overwrite):
            key_name = file
        self.client.upload_file(filepath, bucket, key_name)
        return key_name

    def download(self, bucket, file, filepath):
        self.client.download_file(bucket, file, filepath)

    def download_directory(self, bucket, prefix, path):
        objects = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in objects['Contents']:
            file_name = obj['Key']
            path_to_file = os.path.dirname(file_name)
            os.makedirs(os.path.join(path, path_to_file), exist_ok=True)
            self.download(bucket, file_name, os.path.join(path, file_name))

    def upload_stream(self, bucket, file, data):
        key_name = storage.unique_name(file)
        self.client.upload_fileobj(data, bucket, key_name)
        return key_name

    def download_stream(self, bucket, file):
        data = io.BytesIO()
        self.client.download_fileobj(bucket, file, data)
        return data.getbuffer()

    def get_object(self, bucket, file):
        obj = self.client.get_object(Bucket=bucket, Key=file)
        return obj['Body'].read().decode('utf-8')

    def get_instance():
        if storage.instance is None:
            storage.instance = storage()
        return storage.instance

    def list_blobs(self, bucket):
        res = self.client.list_objects(Bucket=bucket)

        objs = []
        for obj in res['Contents']:
            objs.append(obj['Key'])

        return objs
