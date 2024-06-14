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
    hook = PostgresHook(postgres_conn_id='redshift_morning_slack') 
    return hook.get_conn().cursor()


def get_accessToken():
    # 좌표변환을 위한 accessToken 발급 API
    auth_url = "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json"
    consumer_key = Variable.get("consumer_key")
    consumer_secret = Variable.get("consumer_secret")
    auth_params = {'consumer_key':consumer_key, 'consumer_secret':consumer_secret}
    auth_res = requests.get(auth_url, params=auth_params)
    accessToken = auth_res.json()["result"]["accessToken"]

    return accessToken


@task
def get_user_location(schema):
    cur = get_Redshift_connection()
    table = "user_location"

    query = f"SELECT address, latitude, longitude, location_type FROM {schema}.{table}"

    try:
        cur.execute(query)
        records = cur.fetchall()
        logging.info(records)    
    except Exception as e:
        logging.info(f"Error executing {query}: {e}")
        raise

    return records 


@task
def transform_coordinates(data):
    accessToken = get_accessToken()
    transformed_data = []

    for d in data:
        address, latitude, longitude, location_type = d
        # 좌표변환 API 
        tm_url = "https://sgisapi.kostat.go.kr/OpenAPI3/transformation/transcoord.json"
        tm_params = {
            'accessToken': accessToken,
            'src': 4326,  # WGS84 경/위도
            'dst': 5181,  # 중부원점 GRS80
            'posX': longitude,
            'posY': latitude
        }

        tm_res = requests.get(tm_url, params=tm_params)
        result = tm_res.json()["result"]
        tmX, tmY = result["posX"], result["posY"]
        transformed_data.append([address, latitude, longitude, tmX, tmY, location_type])
    
    return transformed_data

@task
def find_station(data):
    station_data = []
    # 근접측정소 목록 조회 API
    fs_url = "http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getNearbyMsrstnList"
    fs_api_key = Variable.get("fs_api_key")

    for d in data:
        address, latitude, longitude, tmX, tmY, location_type = d
        fs_params =  {
            'serviceKey': fs_api_key,
            'returnType':'json', 
            'tmX': tmX,
            'tmY': tmY,
            'ver': 1.1
            }

        res = requests.get(fs_url, params=fs_params)
        station_name = res.json()["response"]["body"]["items"][0]["stationName"]
        station_data.append([address, latitude, longitude, tmX, tmY, station_name, location_type])

    return station_data


@task
def insert_data(schema, data):
    cur = get_Redshift_connection()
    table = "user_location_station"
    
    # full-refresh
    create_table_query = f"""
    DROP TABLE IF EXISTS {schema}.{table};
    CREATE TABLE {schema}.{table} (
    id int primary key, 
    address varchar(255), 
    latitude float, 
    longitude float, 
    tmX float, 
    tmY float, 
    stationName varchar(100), 
    location_type varchar(20),
    created_date timestamp default GETDATE()
    );
    """

    insert_query = f"""
    INSERT INTO {schema}.{table} (id, address, latitude, longitude, tmX, tmY, stationName, location_type) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

    try:
        cur.execute(create_table_query)
        logging.info(create_table_query)
        logging.info(insert_query)

        for d in data:
            if d[-1] == "origin":
                d.insert(0, 1)
                cur.execute(insert_query, tuple(d))
                logging.info(d)
            elif d[-1] == "destination":
                d.insert(0, 2)
                cur.execute(insert_query, tuple(d))
                logging.info(d)

        cur.execute("COMMIT;")

    except Exception as e:
        logging.info("Exception raise:{e}")
        raise


with DAG(
    dag_id = 'find_station',
    description = 'ETL for finding fine dust station',
    schedule_interval = '* 2 * * *',  # 매일 2시에 작동
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 6, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}) as dag:
    
    schema = "morningslack"

    records = get_user_location(schema)
    transformed_data = transform_coordinates(records)
    station_data = find_station(transformed_data)
    insert_data = insert_data(schema, station_data)

    records >> transformed_data >> station_data >> insert_data