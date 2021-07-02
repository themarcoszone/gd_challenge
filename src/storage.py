from google.cloud.storage import (Client)


class Storage:
    def __init__(self, bucket):
        self.bucket = Client().get_bucket(bucket)

    def upload_file(self, blob_path, local_path):
        blob = self.bucket.blob(blob_path)
        blob.upload_from_filename(local_path)

    def upload_str_to_file(self, csv_str, blob_path):
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(csv_str, 'text/csv')
