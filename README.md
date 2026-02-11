### 背景
本次工作坊主要想法為對資料時效性與自動化的需求。過去的作業模式下，上傳結構化或非結構化數據至雲端時，需仰賴人工監控存儲狀態，並以手動觸發後續的轉檔與分析腳本。
此方式易導致數據處理延遲與資料遺漏的情況。此外，在處理並發任務時，傳統工作流的資源瓶頸易導致障礙。故實作以事件驅動架構 (Event-Driven) 結合定時排程作業
，並利用雲端原生的自動化機制，來確保運算與存儲事件達成即時同步。

<br>
<br>

### 工具說明

- Python 
- Google Cloud Platform (Cloud Storage, Composer, PubSub, BigQuery, VertexAI, BigQuery ML)
<br>
<br>


### 架構圖
![](./pipeline.png)
<br>


### 流程說明
本實作採用監控與執行分離的想法，分成監測 `Sensor DAG` 與資料處理 `Worker DAG` 兩個模式 ，確保流程的可用與擴展性。

1. Sensor DAG (sensor_dag_gcs)

    - 專注於外部事件，整合 PubSub 服務，利用 `PubSubPullSensor` 對訂閱項目的即時監控。當偵測到 GCS 有新檔案產生的訊息時，會一次性抓取最多 10 則訊息，並將這些訊息封裝後傳遞給下游。

2. Worker DAG (worker_dag_processing)

    - 負責處理實際的資料轉換與載入。被 `Sensor DAG` 觸發啟動，從 dag_run.conf 中取出訊息內容，並根據訊息中的檔案路徑進行處理。主要透過 `TriggerDagRunOperator` 將 PubSub 攜帶的事件資料送入 `Worker DAG`。
<br>
<br>


### 淺談 Slot 與 Worker
在探討 Airflow 如何調度任務之前，須先從任務執行開始說起。在 Airflow 的架構中，負責執行程式碼的被稱為 Worker。無論 Worker 是一台實體伺服器、一個虛擬機，還是一個容器，它的資源也就是 CPU 與記憶體都是有限的。若不加限制地讓 Worker 同時啟動多個繁重的運算任務，這台伺服器很快就會因為資源耗盡而崩潰，導致系統當機或觸發記憶體溢出（OOM）的錯誤。為了保護 Worker 的穩定運作，須為它設定一個同時執行任務的上限。預先劃分好這台 Worker 體力範圍內能承載的工作數量，確保它不會超載。而這個受限的「執行名額」，在 Airflow 中稱為 Slot。
<br>
<br>
簡言之，一個 Slot 代表了一個任務執行的許可權。當 Worker 啟動時，它會根據設定開放特定數量的 Slots（e.g. 8 個或 32 個）。每一個準備執行的任務實例（Task Instance），都必須先佔領一個空的 Slot，才能獲取 Worker 的運算資源開始運作。
<br>
<br>
這種機制建立了一道防線：當所有 Slot 都被佔用時，後續的任務就必須在佇列中排隊。這確保了 Worker 永遠只會在自己「吃得下」的範圍內工作，從而維持了整個數據管線的穩定。假設將一台 Worker 伺服器的並發上限設定為 8 個 Slots，這在底層運作中代表這台伺服器最多只能同時啟動並負擔 8 個獨立的進程。每一個進程都代表一個正在運行的任務，它們會瓜分這台機器的 CPU 算力與記憶體空間。
<br>
<br>


### Slot 的佔用
想像以下情境，當啟動了 8 個 Sensor 任務，它們的工作是監控某個外部資料是否到位。由於資料尚未出現，這 8 個任務並未進行實質的數據運算，而是進入了「休眠等待」的循環。然而，在 Airflow 的機制中，只要任務還沒結束，它就必須死死地佔住一個 Slot。上述情況，會導致一個極端矛盾的現象。從硬體監控來看，你的伺服器 CPU 負載極低，因為 Worker 們都在「睡覺」；但從調度系統來看，這台 Worker 已經處於**滿員**狀態。以下舉例說明一般 Sensor 的「佔位」問題 。

    - 動作：工人（Worker）進入工位（Slot），開始打電話問：「資料來了沒？」

    - 頻率：如果資料沒來，工人不會離開，而是坐在工位上發呆（Sleep），等 30 秒後再問一次。

    - 結果：即使工人只是在「等」，他也死死佔住了那個工位。

這導致了資源浪費： 如果任務需要等 2 小時資料才會來，這 2 小時內，那個 Slot 就是停擺的狀態，這時，若有一個真正重要、且只需 10 秒就能跑完的任務準備執行，它也會因為「沒有空的 Slot」而卡在排隊佇列中動彈不得。假設這種 Sensor 多開幾個，Airflow 就算沒在做事，也會因為工位滿了而無法執行任何新任務。

<br>

### 限制
為了避免上述長時間佔用 Worker slot 問題，本實作在 `PubSubPullSensor` 參數中，設定 `deferrable = True`，此功能將等待任務移交給 Airflow Triggerer 處理。
這代表在等待 PubSub 訊息傳入的過程中，不會消耗任何 Worker 運算資源，極大化系統的可用性。
<br>
<br>


### Deferrable Mode

提交監控：Sensor 啟動，告訴系統：要監控這個 PubSub 訂閱。

釋放工位 (Suspend)：Sensor 交出 Slot，工人（Worker）可以去處理其他的資料轉換或運算任務。

交給 Triggerer：監控的工作移交給了一個組件叫 Triggerer。這個組件專門用來處理非同步的等待，一個 Triggerer 可以同時監控成千上萬個任務。當 Pub/Sub 真的有訊息進來時，Triggerer 會發訊號給 Airflow：「資料來了，請重新分配一個 Slot 給這個任務繼續執行。
<br>
<br>


### 程式說明
worker_dag_processing 主要包含三個主要任務：

1. parse_data_info (PythonOperator)

    - 解析外部觸發傳入的 PubSub 訊息。

    - 從 dag_run.conf 中取出訊息，進行 Base64 解碼與 JSON 解析，從中提取檔案所在的 bucket 名稱與檔案 name。

2. load_to_bq (GCSToBigQueryOperator)

    - 將檔案從 GCS 載入到 BigQuery 暫存表。

    - 利用 XCom 動態接收上一個任務解析出來的檔案路徑，並使用 WRITE_APPEND 模式存入 incoming_files-2 資料表，開啟 autodetect 自動辨識欄位格式。

3. generate_embeddings (BigQueryInsertJobOperator)

    - 利用 BigQuery ML 產生資料向量 (Embedding)。

    - 將評論資料（評分、產品ID、摘要、內容）組合成一個長字串 content。

    - ML.GENERATE_EMBEDDING 函式，搭配指定的模型（如 Vertex AI 的嵌入模型）。將產生的向量資料存入最終表 embedded_table_2，供後續語意搜尋或 RAG 應用使用。


### 實做

```bash
git clone https://github.com/JeWeiLin/data-workshop.git

pip3 install -r requirements.txt
```
<br>
<br>


- 設定 Cloud Platform 專案

```bash
gcloud config set project <your-project-ID>
```
<br>
<br>

- 在 Cloud Storage 中建立一個儲存桶，作為存放資料的地方。

```bash
gcloud storage buckets create gs://your-bucket-name --location=asia-east1
```   
<br>
<br>

- 在 PubSub 中建立名為 gcs-file-topic 的主題 (Topic, e.g. gcs-file-topic)

```bash
gcloud pubsub topics create your-pubsub-topic
```  
<br>
<br>

- 建立訂閱 (Subscription)，讓服務可以接收訊息 (e.g. gcs-file-subscription)

```bash
gcloud pubsub subscriptions create your-pubsub-subscription \
    --topic=your-pubsub-topic \
    --ack-deadline=80
```  
<br>
<br>

- 建立 GCS 通知連結 (Notification) ，讓 GCS 在檔案變動時主動發訊息給 PubSub。

```bash
gcloud storage buckets notifications create gs://your-bucket-name \
    --topic=your-pubsub-topic \
    --event-types=OBJECT_FINALIZE
```
<br>

OBJECT_FINALIZE: 代表只有 `新檔案上傳成功` 或 `覆蓋檔案` 時才會發通知。
若有出現 kind: storage#notification 和 id: '1'，代表已經成功建立了 GCS 與 PubSub 之間的通知連結 (Notification) 。


- 建立 Composer 環境 
<br>

- 上傳程式 `gcs_sensor_dag.py` 和 `worker_dag_processing.py` 到 Composer 在 GCS 產生的儲存桶中的 DAGs 資料夾中。

```bash
gcloud storage cp gcs_sensor_dag.py worker_dag_processing.py  gs://your-composer-bucket/dags
```
<br>


- 開啟 Composer 環境中的 Airflow UI 


- 上傳檔案至先前在 Cloud Storage 中建立存放資料的儲存桶。


- 在 BigQuery 中建立 Dataset (e.g. internal_data_demo)


- 在 BigQuery Explorer 中的 Connection 中 建立連線 
    - Connection type 選擇 Vertex AI remote models, remote functions, BigLake and Spanner (Cloud Resource)
    - 創建 Connection ID


- 在 BigQuery 中，執行以下向量搜尋的 SQL，會在先前建立的 Dataset 中產生 Model (embedding_model_2) 與向量表 (embedded_table_2)。
存放經過 Vertex AI 模型處理後的向量資料。針對使用者的自然語言提問，從向量資料庫中檢索出最相關的評論。


```SQL
SELECT 
  base.asin,
  base.reviewerName,
  base.overall,
  base.summary,
  base.reviewText,
  distance -- 數值越小代表語意越接近 (如果是用 Cosine Distance)
FROM VECTOR_SEARCH(
  -- 指定你的向量資料表
  TABLE `tw-rd-data-jewei-lin.internal_data_demo.embedded_table_2`,
  'vector_data',
  
  -- 將問題轉成向量
  (
    SELECT ml_generate_embedding_result, content
    FROM ML.GENERATE_EMBEDDING(
      MODEL `tw-rd-data-jewei-lin.internal_data_demo.embedding_model_2`,
      (SELECT 'I want a reliable SD card with zero issues for my Samsung phone' AS content),
      STRUCT(TRUE AS flatten_json_output)
    )
  ),
  
  -- 回傳前 3 名最相關的評論
  top_k => 3,
  distance_type => 'COSINE'
)
```



1. ML.GENERATE_EMBEDDING (查詢向量化)：

    - 將使用者的提問（e.g. 我想要一張用於三星手機且穩定無誤的 SD 卡）即時轉換成數值向量。

    - 使用與建立資料表時相同的模型 (embedding_model_2)，確保兩者在相同的語意空間中比較。

2. VECTOR_SEARCH (向量比對)：

    - 目標表：embedded_table_2。

    - 搜尋欄位：vector_data（存放評論向量的欄位）。

    - 比對方式：distance_type => 'COSINE'。餘弦相似度常用於文本比對，計算兩個向量之間夾角的餘弦值。

    - top_k => 3：回傳關聯性最高的前 3 筆資料。

    - distance：自動回傳的欄位，數值越小（越接近 0）代表語意越接近使用者的問題。


###  [Slide](https://docs.google.com/presentation/d/12Bul7mEgNAPgvxsmBaKheew2E1whUR-izYS7-27uwzI/edit?slide=id.g35e0a240de4_0_0#slide=id.g35e0a240de4_0_0)
