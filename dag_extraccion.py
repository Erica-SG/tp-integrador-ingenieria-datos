from datetime import datetime
import csv
import os

import requests
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator


def extraer_y_guardar_en_minio():

    # 1. Llamamos a la API
    respuesta = requests.get(
        "https://randomuser.me/api/?results=10",
        timeout=30
    )

    respuesta.raise_for_status()

    usuarios = respuesta.json()["results"]

    # 2. Creamos el CSV
    #“Quiero que mi archivo CSV se llame random_users.csv y lo voy a guardar temporalmente en /tmp.”
    ruta_local = "/tmp/random_users.csv"

    #Esto es una lista de Python. Estamos definiendo cuáles van a ser las columnas del CSV.
    campos = [
        "uuid",
        "nombre",
        "apellido",
        "email",
        "pais",
        "ciudad",
        "edad"
    ]
    #Abrí un archivo en ruta_local para escribir ("w") y llamalo archivo dentro de este bloque.
    with open(ruta_local, "w", newline="", encoding="utf-8") as archivo:
        
        #usamos el módulo csv que importamos al principio.
        #DictWriter es una herramienta de csv que 
        #nos permite escribir el CSV usando diccionarios de Python.
        #“Quiero escribir en archivo y mis nombres de columnas son los que están en campos.”
        writer = csv.DictWriter(
            archivo,
            fieldnames=campos
        )
        
        #crea un fila csv para cada usuario
        #Esto escribe la primera fila, es decir, los encabezados:
        writer.writeheader()


        #recorremos “Para cada usuario que obtuvimos de la API…”
        for usuario in usuarios:

            #Esto crea una fila del CSV por cada usuario.
            writer.writerow({
                "uuid": usuario["login"]["uuid"],
                "nombre": usuario["name"]["first"],
                "apellido": usuario["name"]["last"],
                "email": usuario["email"],
                "pais": usuario["location"]["country"],
                "ciudad": usuario["location"]["city"],
                "edad": usuario["dob"]["age"]
            })

    # 3. Conectamos con MinIO
    cliente = boto3.client(             #crea un cliente S3 usando boto3. boto3 es la librería de Python que nos permite hablar con S3.
        "s3",
        endpoint_url=os.getenv(         #getenv es un método/función del módulo os que permite leer una variable de entorno.
            "MINIO_ENDPOINT",           #“Buscá una variable llamada MINIO_ENDPOINT.”
            "http://minio:9000"         #si exite usa su valor y si no exite usa "http://minio:9000"
        ),
        #CREDENCIALES DE MINIO
        #Es decir, no escribimos usuario y contraseña directamente en el DAG.
        #Los sacamos del .env.Eso es justamente lo que queremos para no tener:   
        #usuario = "minioadmin"
        #contraseña = "minioadmin"

        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),  #busca la variable de entorno MINIO_ROOT_USER 
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"), #busca la variable de entorno MINIO_ROOT_PASSWORD 
        region_name="us-east-1" 
    )
    #defino la región que usara el cliente S3
    #Esto es necesario para que boto3 funcione correctamente, aunque no estemos usando AWS.
    #¿Y region_name="us-east-1"? Esto puede resultar raro porque nosotros no estamos usando AWS.
    #S3 requiere una región en determinados contextos y boto3 espera ese parámetro. 
    #Para MinIO podemos utilizar un valor convencional como:
    #region_name="us-east-1"
    #No significa que nuestro MinIO esté físicamente en Estados Unidos. Es simplemente la región que estamos configurando para el cliente S3.

    #que hace este bloque de código?
    #boto3
    #crea cliente S3
    #lo configura para hablar con
    #MinIO
    #usando endpoint + usuario + contraseña

    # 4. Bucket rawdata
    bucket = "rawdata" #define el nombre del bucket en MinIO

    try:    # TRY: intenta hacer algo y si no funciona pasa al EXCEPT
        cliente.head_bucket(Bucket=bucket) #cliente.head_bucket() es un método de boto3 que permite verificar si un bucket existe. Si no existe, lanza una excepción.
    except Exception:
        cliente.create_bucket(Bucket=bucket) #cliente.create_bucket() es un método de boto3 que permite crear un bucket. Si el bucket ya existe, lanza una excepción.

    # 5. Subimos el CSV a MinIO
    cliente.upload_file(
        ruta_local,             #ruta_local es la ruta del archivo que queremos subir a MinIO. En este caso, es el archivo CSV que creamos en el paso 2.
        bucket,
        "random_users.csv"      #"random_users.csv" es el nombre con el que queremos guardar el archivo en MinIO. En este caso, le estamos diciendo que lo guarde con el mismo nombre que tiene localmente.
    )



with DAG(                               #Acá estamos creando/configurando nuestro DAG., DAG es una clase de Airflow que representa un DAG. Un DAG es un conjunto de tareas que se ejecutan en un orden específico.
    dag_id="extraccion_random_users",   #dag_id es el nombre que le vamos a dar a nuestro DAG. Este nombre tiene que ser único dentro de Airflow.
    start_date=datetime(2026, 1, 1),    #star_date es la fecha en la que queremos que Airflow empiece a ejecutar nuestro DAG. En este caso, le estamos diciendo que empiece a ejecutarlo el 1 de enero de 2026.
    schedule=None,                      #shedule es la frecuencia con la que queremos que Airflow ejecute nuestro DAG. En este caso, le estamos diciendo que no lo ejecute automáticamente, sino que lo ejecutemos manualmente.                    
    catchup=False,                      #catchup es un parámetro que le dice a Airflow si queremos que ejecute todas las ejecuciones pasadas que no se hayan ejecutado. En este caso, le estamos diciendo que no lo haga.
    tags=["extraccion", "minio"],       #tags es una lista de etiquetas que le podemos poner a nuestro DAG para poder filtrarlo en la interfaz web de Airflow.
) as dag:

    extraer = PythonOperator(          #pythonOperator es una clase de Airflow que permite crear una tarea que ejecuta una función de Python. En este caso, estamos creando una tarea que ejecuta la función extraer_y_guardar_en_minio.
        task_id="extraer_api_y_guardar_minio",              #task_id es el nombre que le vamos a dar a nuestra tarea. Este nombre tiene que ser único dentro del DAG.
        python_callable=extraer_y_guardar_en_minio          #python_callable es el parámetro que le dice a PythonOperator qué función de Python queremos que ejecute. En este caso, le estamos diciendo que ejecute la función extraer_y_guardar_en_minio.
    )