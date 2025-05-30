from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix

LOCAL_FILE_PATH = "/opt/airflow/data/sample.txt"
CONTAINER_NAME = "datalake" # airflow
WASB_CONN_ID = "utec_blob_storage"
BLOB_NAME = "raw/airflow/G6/archivo_subido.txt"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id="g0_load_data",
    description="Uploads a local file to Azure Blob Storage with a date suffix.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 9 * * *", # Runs every day at 9:00 am local time (GMT-5)
    catchup=False,
    tags=["utec", "blob", "upload"],
)

def upload_dag():
    @task
    def call_upload():
        new_blob_name = add_date_suffix(BLOB_NAME)
        upload_to_adls(
            local_file_path=LOCAL_FILE_PATH,
            container_name=CONTAINER_NAME,
            blob_name=new_blob_name,
            wasb_conn_id = WASB_CONN_ID
            )

    call_upload()

dag = upload_dag()
