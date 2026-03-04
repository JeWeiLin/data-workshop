from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


with DAG(
    dag_id = 'sensor_dag_gcs',
    start_date = datetime(2024, 1, 1),
    schedule_interval = None,
    catchup = False,
    max_active_runs = 1
) as dag:
    
    pull_messages = PubSubPullSensor(
        task_id = 'pull_messages',
        project_id = 'your-project-id',
        subscription = 'your-subscription',
        max_messages = 10, 
        ack_messages = True, 
        deferrable = True, 
        poke_interval = 30 
    )

    trigger_process = TriggerDagRunOperator(
        task_id = 'trigger_worker_dag',
        trigger_dag_id = 'worker_dag_processing',
        conf = {'messages': "{{ task_instance.xcom_pull(task_ids = 'pull_messages') }}"}
    )

    pull_messages >> trigger_process