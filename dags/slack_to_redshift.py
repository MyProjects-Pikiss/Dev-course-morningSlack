from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import re
from sqlalchemy import create_engine
import json
import os
import hashlib

# Slack과 Redshift 연결 설정
SLACK_BOT_TOKEN = Variable.get("slack_token")  # Variable에서 Slack 토큰을 불러옴
redshift_connection = BaseHook.get_connection("redshift_morning_slack")

REDSHIFT_CONN_STRING = f"redshift+psycopg2://{redshift_connection.login}:{redshift_connection.password}@{redshift_connection.host}:{redshift_connection.port}/{redshift_connection.schema}"
CHANNEL_ID = "C075T29797Z"
SLACK_COMMAND = "적재"
SCHEMA = "morningslack"
TABLE = "user_data"
EXECUTE_TIME = '0 1 * * *'

# Slack 메시지를 파싱하는 함수
def parse_message(text):
    pattern = r"\(([^,]+),\s*([^,]+),\s*([^,]+)\)"
    match = re.search(pattern, text)
    if match:
        return match.groups()
    return None

# Redshift에 메시지를 적재하는 함수
def post_message_to_redshift(s_address, e_address, time):
    engine = create_engine(REDSHIFT_CONN_STRING)
    with engine.connect() as connection:
        try:
            # 테이블이 존재하면 삭제하고 새로 생성
            drop_table_query = f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}"
            create_table_query = f"""
            CREATE TABLE {SCHEMA}.{TABLE} (
                s_address VARCHAR(255),
                e_address VARCHAR(255),
                time VARCHAR(255)
            )
            """
            connection.execute(drop_table_query)
            connection.execute(create_table_query)
            
            # 데이터 삽입
            insert_query = f"INSERT INTO {SCHEMA}.{TABLE} (s_address, e_address, time) VALUES ('{s_address}', '{e_address}', '{time}')"
            connection.execute(insert_query)
            print(f"Data inserted into Redshift: s_address={s_address}, e_address={e_address}, time={time}")
        except Exception as e:
            print(f"Error inserting data into Redshift: {e}")

# 이미 처리된 메시지를 추적하기 위한 세트
processed_messages = set()

# Slack 메시지를 처리하는 함수
def handle_slack_message(event):
    text = event.get("text")
    ts = event.get("ts")
    
    # 메시지 텍스트와 타임스탬프를 해시하여 중복 확인
    message_hash = hashlib.sha256((text + ts).encode()).hexdigest()
    if message_hash in processed_messages:
        print("Message already processed")
        return
    
    if text.startswith(SLACK_COMMAND):
        parsed = parse_message(text[len(SLACK_COMMAND):].strip())
        if parsed:
            s_address, e_address, time = parsed
            with open('/tmp/latest_message.json', 'w') as f:
                json.dump({"s_address": s_address, "e_address": e_address, "time": time}, f)
            print(f"Message parsed and saved: s_address={s_address}, e_address={e_address}, time={time}")
            processed_messages.add(message_hash)
        else:
            print("Message parsing failed")
    else:
        print("Message does not start with the command")

# Airflow DAG에서 호출되는 함수 - 메시지 수집
def fetch_messages(**kwargs):
    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        response = client.conversations_history(channel=CHANNEL_ID, limit=1000)
        print(f"Fetched messages: {response['messages']}")
        
        # 가장 최신 메시지를 찾기 위해 타임스탬프를 비교
        latest_message = max(response['messages'], key=lambda x: float(x.get('ts', 0)), default=None)
                
        if latest_message:
            handle_slack_message(latest_message)
    except SlackApiError as e:
        print(f"Error fetching conversations: {e.response['error']}")

# Airflow DAG에서 호출되는 함수 - Redshift 적재
def load_to_redshift(**kwargs):
    try:
        file_path = '/tmp/latest_message.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
                print(f"Loaded data from file: {data}")
                post_message_to_redshift(data['s_address'], data['e_address'], data['time'])
        else:
            print("No message to load, file does not exist")
    except Exception as e:
        print(f"Error loading data from file: {e}")

# Airflow DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Airflow DAG 정의
dag = DAG(
    'slack_to_redshift',
    default_args=default_args,
    description='Slack 명령어로 입력된 메시지를 Redshift에 저장',
    schedule_interval=EXECUTE_TIME,
    catchup=False,
)

# Airflow PythonOperator 정의
t1 = PythonOperator(
    task_id='fetch_messages',
    provide_context=True,
    python_callable=fetch_messages,
    dag=dag,
)

t2 = PythonOperator(
    task_id='load_to_redshift',
    provide_context=True,
    python_callable=load_to_redshift,
    dag=dag,
)

t1 >> t2
