from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'lucianofodrini'
}

with DAG(
    dag_id='DAG-Clase-7',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['ingest', 'transform_load']
) as dag:

    inicio_proceso = DummyOperator(
        task_id='inicio_proceso',
    )

    ingest = BashOperator(
        task_id='ingest',
        bash_command='ssh hadoop@172.17.0.2 /usr/bin/sh /home/hadoop/scripts/ingest_7.sh ',
        dag=dag
    )

    transform_load = BashOperator(
        task_id='transform_load',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_load_7.py ',
        dag=dag
    )

    fin_proceso = DummyOperator(
        task_id='fin_proceso',
    )

    inicio_proceso >> ingest >> transform_load >> fin_proceso

if __name__ == "__main__":
    dag.cli()
