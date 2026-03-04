from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
import json
import base64

def parse_pubsub_data(**context):
    try:
        conf = context.get('dag_run').conf
        if not conf or 'messages' not in conf:
            raise ValueError("找不到觸發參數，檢查確認是否由 Sensor 觸發")
            
        messages = conf.get('messages', [])
        if not messages:
            raise ValueError("訊息列表為空")

        first_msg = messages[0]
        payload = base64.b64decode(first_msg['data']).decode('utf-8')
        data = json.loads(payload)
        
        print(f"DEBUG: 解析後的資料為 {data}")
        return data
        
    except Exception as e:
        print(f"解析失敗！錯誤原因: {str(e)}")
        return {"bucket": "your-gcs-bucket", "name": "your-filename.csv"} # 填入檔案上傳 GCS 的位置與檔案名稱
    

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
        destination_project_dataset_table = "your-project-id.your-dataset.your-table", # 在 BigQuery 中建立資料載入的位置
        write_disposition = 'WRITE_APPEND',
        source_format = 'CSV',
        autodetect = True,
        skip_leading_rows = 1
    )

    generate_embeddings = BigQueryInsertJobOperator(
        task_id = 'generate_embeddings',
        # CREATE OR REPLACE TABLE `your-project-id.your-dataset.your-embedding-table` 創建 embedding-table 表
        # FROM ML.GENERATE_EMBEDDING(MODEL `your-project-id.your-dataset.your-embedding-model`) 創建模型 
        # SELECT * FROM `your-project-id.your-dataset.your-incoming-files` BigQuery 中建立資料載入的位置
        configuration = {
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `your-project-id.your-dataset.your-embedding-table` AS 
                    SELECT 
                        * EXCEPT(ml_generate_embedding_result, ml_generate_embedding_statistics),
                        ml_generate_embedding_result as vector_data
                    FROM ML.GENERATE_EMBEDDING(
                        MODEL `your-project-id.your-dataset.your-embedding-model`,
                        (
                            SELECT 
                                *, 
                                -- 針對評論資料進行欄位組合
                                CONCAT(
                                    'Rating: ', CAST(overall AS STRING), ' stars. ',
                                    'Product ID: ', asin, 
                                    '. Summary: ', IFNULL(summary, ''), 
                                    '. Review: ', IFNULL(reviewText, '')
                                ) as content
                            FROM `your-project-id.your-dataset.your-incoming-files`
                            -- 排除掉 reviewText 為空的資料，避免浪費額度
                            WHERE reviewText IS NOT NULL
                        ),
                        STRUCT(TRUE AS flatten_json_output)
                    )
                """,
                "useLegacySql": False,
            }
        }
    )
    parse_task >> load_bq >> generate_embeddings
