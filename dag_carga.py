

from datetime import datetime
import io
import os

import boto3
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# CONFIGURACIÓN DE MINIO
# ============================================================
# MinIO es nuestro Data Lake. En la etapa anterior dejamos allí
# los datos transformados en el bucket "staging".
#
# Como Airflow y MinIO están conectados a la misma red Docker,
# desde los contenedores utilizamos el nombre del servicio
# "minio" y el puerto interno 9000.
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000"
)

MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

BUCKET_STAGING = "staging"
ARCHIVO_STAGING = "STG_random_users.csv"


# ============================================================
# CONFIGURACIÓN DE POSTGRESQL
# ============================================================
# PostgreSQL es el destino de los datos transformados.
#
# IMPORTANTE:
# "postgres" es el nombre del servicio PostgreSQL del
# almacenamiento y "appdb" es la base de datos definida
# en nuestro archivo .env.
#
# No utilizamos localhost porque este código se ejecuta dentro
# del contenedor de Airflow. Para comunicarnos entre contenedores
# utilizamos el nombre del servicio Docker.
# ============================================================

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "appdb")
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"


# ============================================================
# FUNCIÓN PRINCIPAL DE CARGA
# ============================================================
def cargar_datos_en_postgres():
    """
    Lee el archivo transformado desde MinIO (STAGING/Silver),
    lo convierte en un DataFrame de Pandas y lo carga en
    PostgreSQL como una tabla llamada random_users.
    """

    print("========== INICIO DE CARGA ==========")

    # --------------------------------------------------------
    # 1. CONECTAR AIRFLOW CON MINIO
    # --------------------------------------------------------
    # boto3 permite trabajar con MinIO utilizando la API
    # compatible con Amazon S3.
    # --------------------------------------------------------

    print("Conectando con MinIO...")

    cliente = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    print("Conexión con MinIO establecida.")

    # --------------------------------------------------------
    # 2. LEER EL ARCHIVO DE STAGING
    # --------------------------------------------------------
    # El DAG anterior generó:
    #
    # staging / STG_random_users.csv
    #
    # Este archivo representa nuestra capa Silver:
    # datos que ya fueron limpiados y transformados.
    # --------------------------------------------------------

    print(
        f"Leyendo archivo {ARCHIVO_STAGING} "
        f"del bucket {BUCKET_STAGING}..."
    )

    objeto = cliente.get_object(
        Bucket=BUCKET_STAGING,
        Key=ARCHIVO_STAGING
    )

    # --------------------------------------------------------
    # 3. CONVERTIR EL ARCHIVO CSV EN DATAFRAME
    # --------------------------------------------------------
    # El contenido llega desde MinIO como bytes.
    # BytesIO permite tratar esos bytes como un archivo en memoria
    # para que Pandas pueda leerlo directamente.
    # --------------------------------------------------------

    df = pd.read_csv(
        io.BytesIO(objeto["Body"].read())
    )

    print(f"Registros STAGING leídos: {len(df)}")
    print(f"Columnas recibidas: {list(df.columns)}")

    # --------------------------------------------------------
    # 4. CONSTRUIR LA CONEXIÓN CON POSTGRESQL
    # --------------------------------------------------------
    # SQLAlchemy nos permite crear la conexión que Pandas utiliza
    # posteriormente mediante el método to_sql().
    #
    # La conexión queda formada como:
    # usuario:contraseña@host:puerto/base_de_datos
    # --------------------------------------------------------

    print("Conectando con PostgreSQL...")

    conexion = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:"
        f"{POSTGRES_PASSWORD}@{POSTGRES_HOST}:"
        f"{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    print("Conexión con PostgreSQL establecida.")

    # --------------------------------------------------------
    # 5. CARGAR LOS DATOS EN POSTGRESQL
    # --------------------------------------------------------
    # to_sql() crea la tabla automáticamente si no existe.
    #
    # if_exists="replace" significa que, si la tabla ya existe,
    # se reemplaza por la nueva información.
    # Esto resulta práctico para nuestro TP porque cada ejecución
    # trabaja con el conjunto actual de 10 usuarios.
    #
    # index=False evita que Pandas agregue su índice como una
    # columna adicional en PostgreSQL.
    # --------------------------------------------------------

    print("Cargando datos en la tabla random_users...")

    df.to_sql(
        "random_users",
        conexion,
        if_exists="replace",
        index=False
    )

    print("Tabla random_users cargada correctamente.")
    print(f"Cantidad de registros cargados: {len(df)}")
    print("=========== FIN DE CARGA ===========")


# ============================================================
# DEFINICIÓN DEL DAG
# ============================================================
# Este DAG se ejecuta manualmente por ahora (schedule=None).
# Más adelante el DAG Master será el encargado de orquestar
# extracción -> transformación -> carga.
# ============================================================

with DAG(
    dag_id="carga_random_users",
    description="Carga los datos transformados de MinIO en PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["carga", "postgresql", "gold"],
) as dag:

    cargar = PythonOperator(
        task_id="cargar_staging_en_postgres",
        python_callable=cargar_datos_en_postgres
    )