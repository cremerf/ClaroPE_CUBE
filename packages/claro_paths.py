from pathlib import Path
from google.cloud import storage
import os

class ClaroPath():
    def __init__(self, bucket_name: str) -> None:

        self.bucket_name = bucket_name
        self.dataset_name = ""
        self.table_name = ""

        # Directorio base de la carpeta del proyecto. La que contiene a todos los archivos

        self.BASE_DIR = Path(__file__).resolve().parent.parent 

        # Directorio de las credenciales:

        self.CREDENTIALS_DIR = os.path.join(self.BASE_DIR, 'credentials.json')

        self.client = storage.Client(credentials = self.CREDENTIALS_DIR)

        self.BUCKET_DIR = self.client.get_bucket(bucket_or_name = bucket_name)


    def set_bucket_name(self, new_bucket_name) -> None:
        self.bucket_name = new_bucket_name

    def set_table_name(self, new_table_name) -> None:
        self.table_name = new_table_name

    def set_dataset_name(self, new_dataset_name) -> None:
        self.dataset_name = new_dataset_name

