TP Integrador – Ingeniería de Datos

Descripción del proyecto

Proyecto integrador de Ingeniería de Datos que implementa un pipeline ETL para la extracción, transformación, almacenamiento y consulta de datos, utilizando herramientas de procesamiento y almacenamiento en contenedores Docker.

El proyecto utiliza datos obtenidos desde la API pública Random User y desarrolla una arquitectura basada en el enfoque Medallion (Raw, Staging y Master).

Tecnologías utilizadas

* Docker y Docker Compose
* Apache Airflow
* Python
* Pandas
* PostgreSQL
* pgAdmin
* MinIO
* OpenMetadata
* Apache Kafka
* Apache Zookeeper
* Elasticsearch
* Git y GitHub

Arquitectura del proyecto

El proyecto está organizado en tres componentes principales:

* Airflow: orquestación de los procesos ETL.
* Almacenamiento: PostgreSQL, pgAdmin y MinIO.
* Gobierno de datos: OpenMetadata y servicios de soporte.

Pipeline de datos

El proceso se organiza en tres etapas:

1. Extracción – RAW

Se obtienen registros desde la API Random User mediante Python y Requests. Los datos originales se almacenan en formato CSV en MinIO, dentro del bucket rawdata.

2. Transformación – STAGING

Se utiliza Pandas para limpiar y transformar los datos:

* Eliminación de registros incompletos.
* Eliminación de duplicados.
* Normalización de nombres y apellidos.
* Normalización de correos electrónicos.
* Conversión y validación de edades.

El resultado se almacena en formato CSV en el bucket staging.

3. Carga – PostgreSQL

Los datos transformados se cargan en PostgreSQL, en la tabla random_users, para su consulta y análisis.

Orquestación con Apache Airflow

Se desarrollaron cuatro DAG:

* extraccion_random_users
* transformacion_random_users
* carga_random_users
* master_pipeline_random_users

El DAG Master coordina la ejecución secuencial de extracción, transformación y carga.

Persistencia y almacenamiento

* MinIO: almacenamiento de archivos del Data Lake.
* PostgreSQL: persistencia de los datos procesados.
* pgAdmin: administración y consulta de la base de datos.

Gobierno de datos

Se incorporó OpenMetadata como herramienta de gobierno y catálogo de datos.

Se configuró el servicio de PostgreSQL correspondiente al proyecto. La ejecución de ingestas desde la interfaz de OpenMetadata quedó pendiente de completar por la integración con las Managed Airflow APIs.

Estado del proyecto

* Extracción desde API.
* Almacenamiento de datos RAW en MinIO.
* Transformación de datos con Pandas.
* Almacenamiento de datos STAGING en MinIO.
* Carga de datos en PostgreSQL.
* Persistencia y verificación mediante pgAdmin.
* Orquestación del pipeline mediante DAG Master.
* Configuración inicial del servicio PostgreSQL en OpenMetadata.
* Finalización de la integración de ingestas de OpenMetadata.

Autora

Erica Soledad Gimeno

Proyecto académico – Ingeniería de Datos.
