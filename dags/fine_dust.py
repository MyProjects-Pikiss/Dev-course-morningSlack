from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_morning_slack')  # 팀프로젝트 connection으로 변경 필요 
    return hook.get_conn().cursor()


def get_locations():
    user_table = 'user_data'
    query = f'SELECT * FROM {user_table}'
    get_stationName_query = f'SELECT stationname FROM user_location_station WHERE id = (%s)'

    cur = get_Redshift_connection()
    try:
        cur.execute(query)
        logging.info(query)
        data = cur.fetchall()[0][:2]
        records = {address: idx+1 for idx, address in enumerate(data)} # {s_address:1, e_address:2} 

        locations = []
        for address, id in records.items():
            if address.startswith('서울'): # 주소가 서울이면, 구 이름 append
                locations.append([address.split()[1], id])
            else:  # 그 이외의 지역이면 측정소 이름 가져오기
                cur.execute(get_stationName_query, (id,))
                stationName = cur.fetchone()
                locations.append([stationName, id])

        return locations
    
    except Exception as e:
        logging.info(f"Error executing {query}: {e}")
        raise


def evaluate_finedust(value):
    if value < 0: return "통신장애"
    elif value == 1: return "좋음"
    elif value == 2: return "보통"
    elif value == 3: return "나쁨"
    elif value == 4: return "매우나쁨"


@task
def extract(location):
    logging.info(f"Extracting data for location: {location}")
    api_key = Variable.get('fine_dust_api_key')
    url = Variable.get('fine_dust_api_url')
    params ={
        'serviceKey' : api_key, 
        'returnType' : 'json', 
        'numOfRows' : '100', 
        'pageNo' : '1', 
        'stationName' : location, 
        'dataTerm' : 'DAILY', 
        'ver' : '1.1' 
        }

    # 이전 23시간의 기록 가져오기
    response = requests.get(url, params=params)
    return response.json()["response"]["body"]["items"]


@task
def transform(id, location, extract_data):
    logging.info(f"Transforming data for location: {location}")
    transformed_data = []

    for d in extract_data:
        keys_to_extract = ['pm10Value', 'pm25Value', 'pm10Grade', 'pm25Grade', 'pm10Value24', 'pm25Value24']
        data = [int(d.get(key, -1)) if d[key].isdigit() else -1 for key in keys_to_extract] # 통신 장애가 발생한 경우 '-' 값은 -1로 변환

        # pm10Grade, pm25Grade 
        data[2] = evaluate_finedust(data[2])
        data[3] = evaluate_finedust(data[3])

        # dataTime 값 수정
        dataTime_str = d["dataTime"]

        # '24:00' 처리
        if '24:00' in dataTime_str:
            date_part = dataTime_str.split(' ')[0]
            # 날짜를 하루 증가시키고 시간을 00:00으로 설정
            new_date_time = datetime.strptime(date_part, '%Y-%m-%d') + timedelta(days=1)
            new_date_time = new_date_time.strftime('%Y-%m-%d 00:00')
            data.append(new_date_time)
        else:
            # datetime 객체로 변환없이 문자열 dataTime append 
            data.append(d["dataTime"])

        data.insert(0, location)  # load에서 편리함을 위해 location 첫번째 인덱스로 insert
        data.insert(0, id)
        transformed_data.append(data)

    return transformed_data

 
@task
def load(location, transformed_data):
    logging.info(f"Loading data for location: {location}") 
    
    cur = get_Redshift_connection()

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id int,
    station_name varchar(50),
    pm10value int,  
    pm25value int,
    pm10grade varchar(20), 
    pm25grade varchar(20),
    pm10value24 int, 
    pm25value24 int, 
    datatime timestamp,
    created_date timestamp default GETDATE()
    );
    """

    empty_check_sql = f"""SELECT EXISTS (SELECT 1 FROM {schema}.{table});"""

    insert_sql = f"""
    INSERT INTO {schema}.{table} (id, station_name, pm10value, pm25value, pm10grade, pm25grade, pm10value24, pm25value24, datatime)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    max_datatime_sql = f"""SELECT max(datatime) FROM {schema}.{table} WHERE station_name = %s;"""
    

    try:
        cur.execute(create_table_sql)
        cur.execute(empty_check_sql)
        exists = cur.fetchone()[0]

        if exists:  # 테이블에 데이터가 있는 경우
            logging.info("not empty table") 
            
            cur.execute(max_datatime_sql, (location,))
            max_datatime = cur.fetchone()[0]  # 현재 테이블에 적재된 데이터 중 가장 최근 시각 
            logging.info(f"Max datatime in table for {location}: {max_datatime}")

            logging.info(insert_sql)

            if max_datatime:  # 해당 location의 데이터가 이미 있는 경우
                for d in transformed_data:
                    if datetime.strptime(d[8],'%Y-%m-%d %H:%M') > max_datatime:  # incremental update 
                        cur.execute(insert_sql, tuple(d))
                        logging.info(d)
            
            else:  # 테이블에 데이터가 있지만 해당 location의 데이터는 없는 경우 
                for d in transformed_data:
                    cur.execute(insert_sql, tuple(d))


        else:  # 테이블이 비어 있는 경우
            logging.info("empty table")
            logging.info(insert_sql)

            for d in transformed_data:
                cur.execute(insert_sql, tuple(d))
        
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise



# DAG 정의
with DAG(
    dag_id = 'ms_fine_dust',
    description='ETL for fine dust data',
    schedule_interval='20 * * * *', # 매 시 20분마다 작동 
    max_active_runs=1,
    catchup = False,
    default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}) as dag:
    
    schema = 'morningslack' 
    table = 'fine_dust'
    locations = get_locations()

    for location, id in locations:
        extract_task = extract.override(task_id=f'extract_{location}')(location)
        transform_task = transform.override(task_id=f'transform_{location}')(id, location, extract_task)
        load_task = load.override(task_id=f'load_{location}')(location, transform_task)

extract_task >> transform_task >> load_task


