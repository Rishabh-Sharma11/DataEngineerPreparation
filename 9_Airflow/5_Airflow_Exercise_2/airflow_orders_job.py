from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14),
}

dag = DAG(
    'orders_backfilling_dag',
    default_args=default_args,
    description='A DAG to run Spark job with input parameter on Dataproc ',
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    }
)

# Python function to get the execution date
def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# PythonOperator to call the get_execution_date function
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# {{ ds_nodash }} = current execution date of dag without dashes

#  2024-07-28

# {{ ds_nodash }} = 20240728

# Dummy BashOperator to print the derived date_variable
# print_date = BashOperator(
#     task_id='print_date',
#     bash_command='echo "Derived execution date: {{ ti.xcom_pull(task_ids=\'get_execution_date\') }}" && echo "hi"',
#     dag=dag,
# )

# {
#     "CLUSTER_NAME" : "hadoop-cluster-new",
#     "PROJECT_ID" : "psyched-service-442305-q1",
#     "REGION" : "us-central1"
# }

# Fetch configuration from Airflow variables
config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']

pyspark_job_file_path = 'gs://airflow-projetcs-gds/airflow-project-2/spark_code/orders_data_process.py'

submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job_file_path,
    arguments=['--date={{ ti.xcom_pull(task_ids=\'get_execution_date\') }}'],  # Passing date as an argument to the PySpark script
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Set the task dependencies
get_execution_date_task >> submit_pyspark_job


# arguments=['--start_date=2024-01-02', '--end_date=2024-01-03']