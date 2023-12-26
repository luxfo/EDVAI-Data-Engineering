from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'lucianofodrini'
}

with DAG(
    dag_id='DAG-TP-1',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['ingest', 'process']
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    ingest_data = BashOperator(
        task_id='ingest',
        bash_command='ssh hadoop@172.17.0.2 /usr/bin/sh /home/hadoop/scripts/tp1_ingest.sh ',
        dag=dag
    )
 
    with TaskGroup(group_id="process_data") as process_data:
        process_1 = BashOperator(
            task_id='process_1',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/tp1_transform_1.py ',
            dag=dag
        )

        process_2 = BashOperator(
            task_id='process_2',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/tp1_transform_2.py ',
            dag=dag
        )

    end = DummyOperator(
        task_id='end',
    )

    start >> ingest_data >> process_data >> end

if __name__ == "__main__":
    dag.cli()
