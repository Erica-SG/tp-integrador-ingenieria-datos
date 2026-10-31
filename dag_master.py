from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# ============================================================
# DAG MASTER - ORQUESTACIÓN DEL PIPELINE
# ============================================================
# Este DAG no realiza directamente la extracción, transformación
# ni carga de datos.
#
# Su función es ORQUESTAR los tres DAG que ya tenemos funcionando:
#
#     1. extraccion_random_users
#             ↓
#     2. transformacion_random_users
#             ↓
#     3. carga_random_users
#
# El Master coordina todo el pipeline sin duplicar el código
# de los procesos anteriores.
# ============================================================


with DAG(
    dag_id="master_pipeline_random_users",
    description=(
        "Orquesta extracción, transformación y carga "
        "de datos de Random User"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["master", "orquestacion", "pipeline"],
) as dag:

    # --------------------------------------------------------
    # 1. EJECUTAR EXTRACCIÓN
    # --------------------------------------------------------
    # Dispara el DAG que consulta la API de Random User
    # y guarda los datos RAW en MinIO.
    # --------------------------------------------------------

    ejecutar_extraccion = TriggerDagRunOperator(
        task_id="ejecutar_extraccion",
        trigger_dag_id="extraccion_random_users",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # --------------------------------------------------------
    # 2. EJECUTAR TRANSFORMACIÓN
    # --------------------------------------------------------
    # Espera que termine la extracción y luego dispara
    # el DAG de transformación.
    #
    # La transformación toma los datos RAW de MinIO,
    # los limpia y genera STAGING.
    # --------------------------------------------------------

    ejecutar_transformacion = TriggerDagRunOperator(
        task_id="ejecutar_transformacion",
        trigger_dag_id="transformacion_random_users",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # --------------------------------------------------------
    # 3. EJECUTAR CARGA
    # --------------------------------------------------------
    # Espera que termine la transformación y luego dispara
    # el DAG de carga.
    #
    # La carga toma STAGING y guarda los datos en PostgreSQL
    # en la tabla random_users.
    # --------------------------------------------------------

    ejecutar_carga = TriggerDagRunOperator(
        task_id="ejecutar_carga",
        trigger_dag_id="carga_random_users",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # ========================================================
    # ORDEN DE EJECUCIÓN
    # ========================================================
    # EXTRAER → TRANSFORMAR → CARGAR
    # ========================================================

    ejecutar_extraccion >> ejecutar_transformacion >> ejecutar_carga