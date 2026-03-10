from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
import json
import base64
import ast


def parse_pubsub_data(**context):
    messages = context.get('dag_run').conf.get('messages', [])

    if isinstance(messages, str):
        try:
            messages = json.loads(messages)
        except json.JSONDecodeError:
            messages = ast.literal_eval(messages.strip())

    first_msg = messages[0]
    if isinstance(first_msg, str):
        first_msg = ast.literal_eval(first_msg.strip())

    # 取得訊息
    msg = first_msg.get('message', first_msg)
    payload = base64.b64decode(msg['data']).decode('utf-8')
    return json.loads(payload)
    

with DAG(
    dag_id = 'worker_dag_processing',
    start_date = datetime(2024, 1, 1),
    schedule_interval = None, # 手動或外部觸發
    catchup = False
) as dag:

    parse_task = PythonOperator(
        task_id = 'parse_data_info',
        python_callable = parse_pubsub_data
    )

    load_bq = GCSToBigQueryOperator(
        task_id = 'load_to_bq',
        bucket = "{{ task_instance.xcom_pull(task_ids = 'parse_data_info')['bucket'] }}",
        source_objects = ["{{ task_instance.xcom_pull(task_ids = 'parse_data_info')['name'] }}"],
        destination_project_dataset_table = "your-project-id.your-dataset.your-incomming-table",
        write_disposition = 'WRITE_APPEND',
        source_format = 'CSV',
        autodetect = True,
        skip_leading_rows = 1,
        allow_quoted_newlines = True,   # 允許欄位內容含換行（Review Text ）
        allow_jagged_rows = True,     # 允許欄位數不足的列
        encoding = 'UTF-8'
    )

    generate_embeddings = BigQueryInsertJobOperator(
        task_id = 'generate_embeddings',
        # CREATE OR REPLACE TABLE `your-project-id.your-dataset.your-embedding-table` 創建 embedding-table 表
        # FROM ML.GENERATE_EMBEDDING(MODEL `your-project-id.your-dataset.your-model`) 創建模型 
        # SELECT * FROM `your-project-id.your-dataset.your-incoming-table` BigQuery 中建立資料載入的位置
        configuration = {
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `your-project-id.your-dataset.your-embedding-table` AS
                    SELECT
                        * EXCEPT(ml_generate_embedding_result, ml_generate_embedding_statistics),
                        ml_generate_embedding_result as vector_data
                    FROM ML.GENERATE_EMBEDDING(
                        MODEL `your-project-id.your-dataset.your-model`,
                        (
                            SELECT
                                *,
                                CONCAT(
                                    'Rating: ', CAST(Rating AS STRING), ' stars. ',
                                    'Country: ', IFNULL(Country, 'Unknown'), '. ',
                                    'Title: ', IFNULL(`Review Title`, ''), '. ',
                                    'Review: ', IFNULL(`Review Text`, ''), '. ',
                                    'Date of Experience: ', IFNULL(`Date of Experience`, '')
                                ) AS content
                            FROM `your-project-id.your-dataset.your-incomming-table`
                            WHERE `Review Text` IS NOT NULL
                        ),
                        STRUCT(TRUE AS flatten_json_output)
                    )
                """,
                "useLegacySql": False,
            }
        }
    )
    parse_task >> load_bq >> generate_embeddings

