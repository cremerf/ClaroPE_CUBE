from pathlib import Path
from google.cloud import storage
import os

class ClaroPath():
    def __init__(self, bucket_name: str, working_dir_name: str) -> None:

        self.bucket_name = bucket_name
        self.dataset_name = ""
        self.table_name = ""

        # Directorio base de la carpeta del proyecto. La que contiene a todos los archivos.

        self.BASE_DIR = Path(__file__).resolve().parent.parent 

        # Directorio de las credenciales:

        self.CREDENTIALS_DIR = os.path.join(self.BASE_DIR, 'credentials.json')

        # Conexion a Google Cloud Storage.

        self.client = storage.Client.from_service_account_json(json_credentials_path = self.CREDENTIALS_DIR)

        # Seteo del bucket: bucket donde se guardaran los archivos.

        self.BUCKET = self.set_bucket()

        # Nombre del directorio(folder/carpeta) dentro del bucket.

        self.WORKING_DIR_NAME = working_dir_name


    def set_bucket(self):

        if not self.client.get_bucket(bucket_or_name = self.bucket_name).exists():
            self.client.create_bucket(bucket_or_name = self.bucket_name)

        else:
            self.client.get_bucket(bucket_or_name = self.bucket_name)

    def set_bucket_name(self, new_bucket_name) -> None:
        self.bucket_name = new_bucket_name

    def set_table_name(self, new_table_name) -> None:
        self.table_name = new_table_name

    def set_dataset_name(self, new_dataset_name) -> None:
        self.dataset_name = new_dataset_name

