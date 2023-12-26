from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'lucianofodrini'
}

with DAG(
    dag_id='DAG-TP-2-CHILD',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['process']
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    process_data = BashOperator(
        task_id='process_data',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/tp2_transform.py ',
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> process_data >> end

if __name__ == "__main__":
    dag.cli()
