from datetime import datetime, timedelta
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# pyrefly: ignore [missing-import]
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'monthly_taxi_etl',
    default_args=default_args,
    description='Monthly ETL from MinIO to Postgres DWH',
    schedule_interval='@monthly', # Runs at the end of the month or beginning of next
    catchup=False,
    tags=['taxi', 'etl', 'dwh'],
) as dag:

    start = DummyOperator(task_id='start')

    # The data_interval_start gives the execution date
    # E.g. for March 2016 data, it will pass year=2016 and month=03
    dwh_etl_job = SparkSubmitOperator(
        task_id='run_spark_dwh_etl',
        application='/opt/airflow/jobs/dwh_etl.py',
        conn_id='spark_default',
        application_args=[
            '--year', '{{ execution_date.strftime("%Y") }}',
            '--month', '{{ execution_date.strftime("%m") }}'
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
        },
        verbose=True
    )

    end = DummyOperator(task_id='end')

    start >> dwh_etl_job >> end
