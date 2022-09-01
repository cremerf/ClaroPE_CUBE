from pathlib import Path
import os

class ClaroPath():
    def __init__(self) -> None:
        # Directorio base de la carpeta del proyecto. La que contiene a todos los archivos:
        self.BASE_DIR = Path(__file__).resolve().parent.parent 
        
        # Directorio de las credenciales:
        self.CREDENTIALS_DIR = os.path.join(self.BASE_DIR, 'credentials.json')
        
        # Directorios de donde toma los datos:
        # Por ahora no es necesario
        
        # Directorio de subida de datos:

