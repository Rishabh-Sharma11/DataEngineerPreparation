from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# Define default_args dictionary to pass default parameters to the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG with the specified default_args
dag = DAG(
    'fetch_json_and_load_to_hive',
    default_args=default_args,
    description='DAG to fetch a daily JSON file from GCP bucket and load into Hive table on Dataproc',
    schedule_interval=timedelta(days=1),  # Set the DAG to run daily
)

# Define the GCS bucket and JSON file details
gcs_bucket = 'airflow_ass2'
gcs_object = 'input_files/Employee.json'
local_directory = '/path/to/local/directory'  # Local directory to store the downloaded file

# Define the Hive table details
hive_table = 'employee'
hive_table_schema = 'emp_id INT, emp_name STRING, dept_id INT, salary INT'
hive_table_location = 'gs://airflow_ass2/hive_data/'

# Define the Hive query to load JSON data into the Hive table
hive_query = f"""
LOAD DATA LOCAL INPATH '/path/to/local/directory/daily_file.json'
OVERWRITE INTO TABLE employee
"""

# Task 1: Use GCSToLocalFilesystemOperator to download the daily JSON file
download_task = GCSToLocalFilesystemOperator(
    task_id='download_from_gcs',
    bucket=gcs_bucket,
    object_name=gcs_object,
    filename='/path/to/local/directory/daily_file.json',
    dag=dag,
)

# Task 2: Use DataprocSubmitJobOperator to submit a Hive job on Dataproc
submit_hive_job = DataprocSubmitJobOperator(
    task_id='submit_hive_job',
    job={
        'reference': {'project_id': 'dev-solstice-403604'},
        'placement': {'cluster_name': 'hadoop-cluster2'},
        'hive_job': {'query_list': {'queries': [hive_query]}},
    },
    region='us-central1',  # Specify the Dataproc region
    project_id='dev-solstice-403604',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Set task dependencies
download_task >> submit_hive_job