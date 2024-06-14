from airflow import DAG
from airflow.macros import *
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime

import os
from glob import glob
import logging
import subprocess
import json
import requests


DAG_ID = "Navigator_v1"
KAKAO_API_KEY = Variable.get("KAKAO_API_KEY")
SCHEMA = Variable.get("PROJECT_SCHEMA")


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


"""
redshift로 부터 사용자의 주소가 담긴 record 불러오는 메서드

return: [(출발지 주소 및 gps 정보들), (목적지 주소 및 gps 정보들)]
"""
def read_meta_from_redshift():
    cur = get_Redshift_connection()
    table = "user_location"

    query = f"SELECT * FROM {SCHEMA}.{table};"

    try:
        cur.execute(query)
        records = cur.fetchall()
        return records
    except Exception as e:
        logging.info(f"Error executing query: {e}")
        raise
    finally:
        cur.close()


"""
record로부터 필요한 정보만 추출하고, api의 형식에 맞게 변환하는 메서드
"""
def read_gps_from(record):
    address = record[1]
    latitude = record[2]
    longitude = record[3]
    logging.info(f"read gps ==> address={address} latitude={latitude}, longitude={longitude}")
    return {
        "address" : address,
        "gps" : f"{longitude},{latitude}"
    }


"""
redshift로부터 출발지와 목적지의 주소 및 gps 정보를 추출하는 task

return:
{
    "origin": {
        "address" : 상세 주소,
        "gps" : "longitude,latitude"
    }, 
    "destination": {
        ...
    }
}
"""
@task
def read_user_location():
    records = read_meta_from_redshift()
    logging.info(records)
    origin = read_gps_from(records[0])
    destination = read_gps_from(records[1])
    logging.info(f"origin gps ==> {origin}")
    logging.info(f"destination gps ==> {destination}")
    return {
        "origin" : origin,
        "destination" : destination
    }


"""
카카오 길찾기 api를 호출하는 task

return:
api 결과에 user_location을 합친 dict
"""
@task
def navi_api(user_location):
    url = "https://apis-navi.kakaomobility.com/v1/directions"
    params = {
        "origin" : user_location["origin"]["gps"],
        "destination" : user_location["destination"]["gps"],
        "waypoints": "",
        "priority": "RECOMMEND",
        "car_fuel": "GASOLINE",
        "car_hipass": "false",
        "alternatives": "false",
        "road_details": "false"
    }
    
    headers = {
        "Authorization": f"KakaoAK {KAKAO_API_KEY}"
    }

    res = requests.get(url, headers=headers, params=params)
    results = json.loads(res.text)

    results["origin"] = user_location["origin"]
    results["destination"] = user_location["destination"]
    logging.info(res.text)
    return results


"""
카카오 길찾기 api 결과 중, 필요한 부분만 추출하는 api

return:
{
    "origin" : {
        "address" : 상세주소,
        "gps" : ...
    }, 
    "destination" : {
        ...
    },
    "duration" : 소요시간 (초)
}
"""
@task
def transform(api_results):
    try:
        duration = api_results["routes"][0]["summary"]["duration"]
        result = {
            "origin" : api_results["origin"],
            "destination" : api_results["destination"],
            "duration" : duration
        }
        logging.info(f"result: {result}")
        return result
    except KeyError as e:
        ## 길찾기 api는 비정상적인 입력을 받게되면 
        ##'routes' key는 존재하지 않고, 'code'라는 key를 가진 결과를 내놓음, 
        if "code" in api_results and "msg" in api_results:
            logging.info(api_results["msg"])
        raise


@task
def upload(transformed_data):
    origin_address = transformed_data["origin"]["address"]
    destination_address = transformed_data["destination"]["address"]
    duration = transformed_data["duration"]
    create_table_sql = f"""
        DROP TABLE IF EXISTS {SCHEMA}.duration_info;
        CREATE TABLE {SCHEMA}.duration_info (
            datetime timestamp,
            origin_address varchar(200),
            destination_address varchar(200),
            duration int
        );
    """
    insert_sql = f"""
        INSERT INTO {SCHEMA}.duration_info VALUES (
            '{datetime.now()}', '{origin_address}', '{destination_address}', '{duration}'
        );
    """

    logging.info(f"insert_sql: {insert_sql}")

    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(create_table_sql)
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.info(e)
        raise
    finally:
        cur.close()


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 8 * * *",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    start_date=datetime(2024, 6, 10),
    default_args= {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    user_location = read_user_location()
    api_results = navi_api(user_location)
    transformed_data = transform(api_results)
    upload(transformed_data)

