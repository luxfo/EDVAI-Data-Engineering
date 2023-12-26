from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'lucianofodrini'
}

with DAG(
    dag_id='DAG-TP-2-PARENT',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['ingest']
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    ingest_data = BashOperator(
        task_id='ingest_data',
        bash_command='ssh hadoop@172.17.0.2 /usr/bin/sh /home/hadoop/scripts/tp1_ingest.sh ',
        dag=dag
    )

    trigger_process_data = TriggerDagRunOperator(
        task_id='trigger_process',
        trigger_dag_id='DAG-TP-2-CHILD',
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> ingest_data >> trigger_process_data >> end

if __name__ == "__main__":
    dag.cli()
