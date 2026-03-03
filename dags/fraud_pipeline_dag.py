from airflow import DAG
# noinspection PyUnresolvedReferences
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        'fraud_detection_end_to_end',
        default_args=default_args,
        description='Pipeline tự động kéo data và đẩy thẳng vào ClickHouse qua S3',
        schedule_interval='@daily',
        catchup=False
) as dag:
    task_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='python /opt/airflow/src/spark_jobs/bronze/bronze_ingestion.py'
    )

    task_silver_dim = BashOperator(
        task_id='process_silver_dim',
        bash_command='python /opt/airflow/src/spark_jobs/silver/silver_dimensions.py'
    )

    task_silver_fact = BashOperator(
        task_id='process_silver_fact',
        bash_command='python /opt/airflow/src/spark_jobs/silver/silver_transaction.py'
    )

    task_gold_join = BashOperator(
        task_id='gold_layer_join',
        bash_command='python /opt/airflow/src/spark_jobs/gold/gold_to_clickhouse.py'
    )

    task_gold_parquet = BashOperator(
        task_id='gold_export_parquet',
        bash_command='python /opt/airflow/src/spark_jobs/gold/push_to_clickhouse_only.py'
    )

    clickhouse_curl_cmd = """
    curl -X POST -u default:admin "http://clickhouse:8123/" -d "TRUNCATE TABLE IF EXISTS fraud_db.gold_transaction_master;" && \
    curl -X POST -u default:admin "http://clickhouse:8123/" -d "INSERT INTO fraud_db.gold_transaction_master SELECT * FROM s3('http://minio:9000/gold/transaction_master_parquet/*.parquet', 'admin', 'Admin123', 'Parquet')"
    """

    task_clickhouse_pull = BashOperator(
        task_id='clickhouse_s3_pull',
        bash_command=clickhouse_curl_cmd
    )

    task_bronze >> [task_silver_dim, task_silver_fact] >> task_gold_join >> task_gold_parquet >> task_clickhouse_pull