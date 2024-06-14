from airflow import DAG
from airflow.macros import *
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

import os
from glob import glob
import logging
import subprocess
import json
import requests

from utils.forecast_grid import grid


DAG_ID = "Location_v1"
KAKAO_API_KEY = Variable.get("KAKAO_API_KEY")
SCHEMA = Variable.get("PROJECT_SCHEMA")


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


"""
카카오 길찾기 api 호출하는 메서드
주소를 입력받아, 그에 맞는 정보를 반환
그 중, 위도와 경도만 추출

input: '서울', '서울시', '서울특별시', '서울 반포4동', '서울 서초구 반포4동' 다양한 형식의 string
return: 위도와 경도를 담은 dict
"""
def gps_api(address):
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    params = {
        "query" : address
    }
    headers = {
        "Authorization": f"KakaoAK {KAKAO_API_KEY}"
    }
    res = requests.get(url, headers=headers, params=params)
    results = json.loads(res.text)
    x = results["documents"][0]["x"]    # 경도 (Longitude)
    y = results["documents"][0]["y"]    # 위도 (Latitude)
    return {
        "longitude" : x,
        "latitude" : y
    }

"""
데이터로부터 Insert query를 작성해주는 메서드
"""
def get_insert_sql(location_data, id):
    sql_format = """
        INSERT INTO {schema}.user_location VALUES (
            '{id}', '{address}', '{latitude}', '{longitude}', '{nx}', '{ny}', '{location_type}'
        );
    """
    return sql_format.format(
        id=id, schema=SCHEMA, address=location_data["address"],
        latitude=location_data["latitude"], longitude=location_data["longitude"],
        nx=location_data["nx"], ny=location_data["ny"],
        location_type=location_data["location_type"]
    )

"""
redshift로부터 사용자의 주소를 읽어오는 task

return: 
{
    "origin_address" : "서울"
    "destination_address" : "부산"
}
"""
@task
def read_cities():
    cur = get_Redshift_connection()
    table = "user_data"

    query = f"SELECT * FROM {SCHEMA}.{table};"

    try:
        cur.execute(query)
        records = cur.fetchall()
        logging.info(records)
        return {
            "origin_address" : records[0][0],
            "destination_address" : records[0][1]
        }
    except Exception as e:
        print(f"Error executing query: {e}")
        raise
    finally:
        cur.close()


"""
카카오 지도 api 호출 task

input: read_cities 반환 결과
return:
{
    "origin_gps" : {
            "longitude" : ...,
            "latitude" : ...
    },
    "destination_gps" : {
            "longitude" : ...,
            "latitude" : ...
    }
}
"""
@task
def city_to_gps(cities):
    origin_gps = gps_api(cities["origin_address"])
    destination_gps = gps_api(cities["destination_address"])
    logging.info(f"origin: {origin_gps}")
    logging.info(f"destination: {destination_gps}")
    return {
        "origin_gps" : origin_gps,
        "destination_gps" : destination_gps
    }


"""
기상청 좌표 변환 task
dags.utils.forecast_gird 를 이용하여 변환

input: city_to_gps 반환 결과
return:
{
    "origin_grid" : {
        "nx" : ...,
        "ny" : ...
    },
    "destination_grid" : {
        "nx" : ...,
        "ny" : ...
    }
}
"""
@task
def gps_to_forecast_grid(gps):
    origin_lat, origin_long = float(gps["origin_gps"]["latitude"]), float(gps["origin_gps"]["longitude"])
    destination_lat, destination_long = float(gps["destination_gps"]["latitude"]), float(gps["destination_gps"]["longitude"])
    origin_grid = grid(latitude=origin_lat, longitude=origin_long)
    destination_grid = grid(latitude=destination_lat, longitude=destination_long)
    logging.info(f"origin: {origin_grid}")
    logging.info(f"destination: {destination_grid}")
    return {
        "origin_grid": origin_grid,
        "destination_grid": destination_grid
    }


@task
def transform(cities, gps_info, grid_info):
    user_location = {
        "origin" : {
            "address" : cities["origin_address"],
            "latitude" : gps_info["origin_gps"]["latitude"],
            "longitude" : gps_info["origin_gps"]["longitude"],
            "nx" : grid_info["origin_grid"]["nx"],
            "ny" : grid_info["origin_grid"]["ny"],
            "location_type" : "origin"
        },
        "destination" : {
            "address" : cities["destination_address"],
            "latitude" : gps_info["destination_gps"]["latitude"],
            "longitude" : gps_info["destination_gps"]["longitude"],
            "nx" : grid_info["destination_grid"]["nx"],
            "ny" : grid_info["destination_grid"]["ny"],
            "location_type" : "destination"
        }
    }
    logging.info(f"user_location: \n{user_location}")
    return user_location

@task
def load_user_location(user_location):
    cur = get_Redshift_connection()
    
    sql_create_table = f"""
        DROP TABLE IF EXISTS {SCHEMA}.user_location;
        CREATE TABLE {SCHEMA}.user_location (
            id int primary key,
            address varchar(200),
            latitude float,
            longitude float,
            nx int,
            ny int,
            location_type varchar(20) -- origin 혹은 destination
        );
    """
    sql_origin_insert = get_insert_sql(user_location["origin"], "1")
    sql_destination_insert = get_insert_sql(user_location["destination"], "2")

    logging.info(f"sql_create_table:\n{sql_create_table}")
    logging.info(f"sql_origin_insert:\n{sql_origin_insert}")
    logging.info(f"sql_destination_insert:\n{sql_destination_insert}")

    try:
        cur.execute("BEGIN;")
        cur.execute(sql_create_table)
        cur.execute(sql_origin_insert)
        cur.execute(sql_destination_insert)
        cur.execute("COMMIT;")
    except Exception as e:
        logging.info(f"Error executing query: {e}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()

with DAG(
    dag_id=DAG_ID,
    schedule_interval="30 1 * * *",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    start_date=datetime(2024, 6, 10),
    default_args= {
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
) as dag:
    cities = read_cities()
    gps_info = city_to_gps(cities)
    gird_info = gps_to_forecast_grid(gps_info)
    user_location = transform(cities, gps_info, gird_info)
    load_user_location(user_location)

