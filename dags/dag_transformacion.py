# ============================================================
# DAG DE TRANSFORMACIÓN - CAPA SILVER / STAGING
# ============================================================
#
# Este DAG toma los datos originales que tenemos en MinIO,
# dentro del bucket "rawdata", y realiza distintas
# transformaciones utilizando Pandas.
#
# Flujo:
#
# API
#   ↓
# DAG EXTRACCIÓN
#   ↓
# MinIO / rawdata       ← CAPA BRONZE
#   ↓
# DAG TRANSFORMACIÓN
#   ↓
# Pandas
#   ↓
# MinIO / staging       ← CAPA SILVER
#
# El archivo original NO se modifica.
# Se genera un nuevo archivo con los datos transformados.
# ============================================================


# ------------------------------------------------------------
# IMPORTACIONES
# ------------------------------------------------------------

import os
# "os" nos permite trabajar con variables de entorno.
# Las utilizamos para obtener las credenciales de MinIO
# sin escribir usuario y contraseña directamente en el código.


import io
# "io" nos permite trabajar con datos en memoria.
# Lo vamos a utilizar para leer y generar archivos CSV
# sin tener que guardarlos primero en el disco.


import boto3
# boto3 es la librería que nos permite comunicarnos
# con servicios compatibles con S3.
#
# MinIO utiliza una API compatible con S3, por eso
# podemos utilizar boto3 para subir y descargar objetos.


import pandas as pd
# Pandas es la librería que vamos a utilizar para
# trabajar con los datos.
#
# "pd" es simplemente un alias.
#
# Por ejemplo:
# pandas.read_csv()
#
# se escribe normalmente:
# pd.read_csv()


from datetime import datetime
# Importamos la clase datetime del módulo datetime.
# La vamos a utilizar para indicar la fecha de inicio
# del DAG.


from airflow import DAG
# Importamos la clase DAG de Airflow.
#
# DAG significa Directed Acyclic Graph.
# Es la estructura que representa nuestro flujo
# de tareas en Airflow.


from airflow.operators.python import PythonOperator
# PythonOperator permite que Airflow ejecute
# una función escrita en Python.


# ============================================================
# CONFIGURACIÓN DE MINIO
# ============================================================

# Endpoint de MinIO.
#
# Primero intentamos obtener una variable de entorno
# llamada MINIO_ENDPOINT.
#
# Si esa variable no existe, utilizamos:
# http://minio:9000
#
# "minio" es el nombre del contenedor dentro de la
# red Docker compartida.
#
# El puerto 9000 es el puerto interno de MinIO
# utilizado para la API.
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000"
)


# Usuario de MinIO.
#
# No escribimos:
#
# minioadmin
#
# directamente en el código.
#
# Lo obtenemos desde la variable de entorno
# MINIO_ROOT_USER, que viene de nuestro .env.
MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ROOT_USER"
)


# Contraseña de MinIO.
#
# También la obtenemos desde una variable de entorno.
# Esto evita hardcodear credenciales en nuestro DAG.
MINIO_SECRET_KEY = os.getenv(
    "MINIO_ROOT_PASSWORD"
)


# Región utilizada por boto3.
#
# Estamos utilizando MinIO, no AWS.
# Pero boto3 trabaja con el modelo de S3 y espera
# una región en determinados contextos.
#
# "us-east-1" es un valor convencional.
AWS_REGION = "us-east-1"


# ============================================================
# BUCKETS Y ARCHIVOS
# ============================================================

# Bucket donde guardamos los datos originales.
#
# Este representa nuestra capa RAW / BRONZE.
BUCKET_RAW = "rawdata"


# Bucket donde vamos a guardar los datos transformados.
#
# Este representa nuestra capa STAGING / SILVER.
BUCKET_STAGING = "staging"


# Nombre del archivo que generó nuestro DAG de extracción.
#
# Recordemos que nuestro DAG de extracción actualmente
# genera:
#
# random_users.csv
#
# y lo guarda dentro de:
#
# MinIO → rawdata
RAW_FILE = "random_users.csv"


# Nombre que tendrá el archivo después de la transformación.
#
# El TP propone utilizar el prefijo STG_ para los archivos
# de la capa STAGING.
STG_FILE = "STG_random_users.csv"


# ============================================================
# FUNCIÓN PRINCIPAL DE TRANSFORMACIÓN
# ============================================================

def transformar_datos():
    """
    Esta función realiza toda la transformación.

    El proceso completo es:

    1. Conectarse a MinIO.
    2. Verificar que exista el bucket staging.
    3. Leer el CSV desde rawdata.
    4. Cargar el CSV en un DataFrame de Pandas.
    5. Limpiar los datos.
    6. Transformar los tipos de datos.
    7. Generar un nuevo CSV.
    8. Subir el CSV transformado a staging.
    """

    # ========================================================
    # 1. CONEXIÓN CON MINIO
    # ========================================================

    # Creamos un cliente de boto3.
    #
    # Este objeto "s3_client" será el encargado de
    # comunicarse con MinIO.

    s3_client = boto3.client(
        "s3",

        # Dirección de MinIO dentro de Docker.
        endpoint_url=MINIO_ENDPOINT,

        # Usuario obtenido desde .env.
        aws_access_key_id=MINIO_ACCESS_KEY,

        # Contraseña obtenida desde .env.
        aws_secret_access_key=MINIO_SECRET_KEY,

        # Región utilizada por boto3.
        region_name=AWS_REGION
    )


    # ========================================================
    # 2. VERIFICAR / CREAR BUCKET STAGING
    # ========================================================

    # Antes de guardar nuestro archivo transformado,
    # necesitamos asegurarnos de que exista el bucket
    # "staging".

    try:

        # head_bucket() consulta si el bucket existe.
        s3_client.head_bucket(
            Bucket=BUCKET_STAGING
        )

        # Si no ocurrió ningún error,
        # significa que el bucket ya existe.
        print(
            f"Bucket {BUCKET_STAGING} ya existe."
        )

    except Exception:

        # Si ocurrió un error al buscar el bucket,
        # intentamos crearlo.
        s3_client.create_bucket(
            Bucket=BUCKET_STAGING
        )

        print(
            f"Bucket {BUCKET_STAGING} creado."
        )


    # ========================================================
    # 3. LEER EL ARCHIVO RAW DESDE MINIO
    # ========================================================

    # get_object() obtiene un objeto almacenado en MinIO.

    raw_object = s3_client.get_object(
        Bucket=BUCKET_RAW,
        Key=RAW_FILE
    )


    # ========================================================
    # 4. CARGAR EL CSV EN PANDAS
    # ========================================================

    # raw_object["Body"] contiene el contenido del archivo.

    # .read() lee ese contenido.

    # io.BytesIO() permite tratar esos bytes como
    # un archivo en memoria.

    # pd.read_csv() transforma el CSV en un DataFrame.

    df = pd.read_csv(
        io.BytesIO(
            raw_object["Body"].read()
        )
    )


    # len(df) nos indica cuántos registros tiene
    # nuestro DataFrame.

    print(
        f"Registros RAW leídos: {len(df)}"
    )


    # ========================================================
    # TRANSFORMACIONES
    # ========================================================

    # --------------------------------------------------------
    # 5. ELIMINAR REGISTROS SIN UUID O EMAIL
    # --------------------------------------------------------

    # dropna() elimina filas que contienen valores nulos.

    # subset indica en qué columnas queremos controlar
    # los valores nulos.

    # Para nuestro proyecto consideramos importantes:
    #
    # uuid
    # email

    df = df.dropna(
        subset=[
            "uuid",
            "email"
        ]
    )


    # --------------------------------------------------------
    # 6. ELIMINAR DUPLICADOS
    # --------------------------------------------------------

    # drop_duplicates() elimina registros repetidos.

    # subset=["uuid"] significa que utilizamos UUID
    # para identificar duplicados.

    # keep="first" significa:
    # si encontramos dos usuarios con el mismo UUID,
    # conservamos el primero.

    df = df.drop_duplicates(
        subset=["uuid"],
        keep="first"
    )


    # --------------------------------------------------------
    # 7. LIMPIAR CAMPOS DE TEXTO
    # --------------------------------------------------------

    # Estas son las columnas de texto que tenemos
    # en nuestro CSV.

    columnas_texto = [
        "nombre",
        "apellido",
        "pais",
        "ciudad"
    ]


    # Recorremos cada columna de texto.

    for columna in columnas_texto:

        df[columna] = (
            df[columna]

            # Convertimos el contenido a texto.
            .astype(str)

            # Eliminamos espacios innecesarios
            # al principio y al final.
            .str.strip()

            # Ponemos la primera letra de cada palabra
            # en mayúscula.
            .str.title()
        )


    # --------------------------------------------------------
    # 8. NORMALIZAR EMAIL
    # --------------------------------------------------------

    # Los emails normalmente se almacenan en minúsculas
    # para evitar diferencias como:
    #
    # Erica@correo.com
    # erica@correo.com
    #
    # .strip() elimina espacios.
    #
    # .lower() convierte todo a minúsculas.

    df["email"] = (
        df["email"]
        .astype(str)
        .str.strip()
        .str.lower()
    )


    # --------------------------------------------------------
    # 9. CONVERTIR EDAD A NÚMERO
    # --------------------------------------------------------

    # pd.to_numeric() intenta convertir los valores
    # de la columna edad a números.

    # errors="coerce" significa que si encuentra un valor
    # que no puede convertirse, lo transforma en NaN
    # (valor nulo de Pandas).

    df["edad"] = pd.to_numeric(
        df["edad"],
        errors="coerce"
    )


    # Eliminamos las filas donde la edad no pudo
    # convertirse correctamente.

    df = df.dropna(
        subset=["edad"]
    )


    # Convertimos la edad a entero.
    #
    # Por ejemplo:
    #
    # 25.0 → 25
    #
    df["edad"] = df["edad"].astype(int)


    # Mostramos cuántos registros quedaron
    # después de las transformaciones.

    print(
        f"Registros procesados y limpios: {len(df)}"
    )


    # ========================================================
    # 10. GENERAR EL CSV DE STAGING
    # ========================================================

    # Creamos un espacio de memoria para nuestro CSV.

    csv_buffer = io.BytesIO()


    # DataFrame → CSV
    #
    # index=False evita que Pandas agregue una columna
    # adicional con el índice del DataFrame.

    df.to_csv(
        csv_buffer,
        index=False,
        encoding="utf-8"
    )


    # Volvemos al comienzo del archivo en memoria.
    #
    # Esto es necesario antes de subirlo.

    csv_buffer.seek(0)


    # ========================================================
    # 11. SUBIR EL CSV TRANSFORMADO A MINIO
    # ========================================================

    # upload_fileobj() permite subir directamente
    # el archivo que tenemos en memoria.

    s3_client.upload_fileobj(
        csv_buffer,

        # Bucket destino:
        BUCKET_STAGING,

        # Nombre del archivo:
        STG_FILE
    )


    # Mensaje para los logs de Airflow.

    print(
        f"Archivo guardado en "
        f"{BUCKET_STAGING}/{STG_FILE}"
    )


# ============================================================
# DEFINICIÓN DEL DAG
# ============================================================

# Ahora creamos el DAG de Airflow.

with DAG(

    # Nombre único del DAG dentro de Airflow.
    dag_id="transformacion_random_users",

    # Fecha desde la cual Airflow considera válido
    # este DAG.
    start_date=datetime(2026, 1, 1),

    # None significa que por ahora NO se ejecuta
    # automáticamente según un horario.
    #
    # Lo vamos a ejecutar manualmente para probarlo.
    schedule=None,

    # No ejecutar automáticamente fechas pasadas.
    catchup=False,

    # Etiquetas que nos ayudan a identificar el DAG
    # dentro de Airflow.
    tags=[
        "transformacion",
        "silver",
        "minio"
    ],

) as dag:


    # ========================================================
    # TAREA DEL DAG
    # ========================================================

    # PythonOperator le indica a Airflow:
    #
    # "Cuando ejecutes esta tarea,
    # ejecutá la función transformar_datos."

    transformar = PythonOperator(

        # Nombre de la tarea que veremos en Airflow.
        task_id="transformar_datos",

        # Acá pasamos la función SIN paréntesis.
        #
        # No queremos ejecutarla ahora.
        #
        # Queremos que Airflow la ejecute cuando
        # corresponda.
        python_callable=transformar_datos
    )
