from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd

def fetch_news(category=None, page_size=20):
    url = 'https://newsapi.org/v2/top-headlines'
    api_key = Variable.get("news_api_key")
    params = {
        'country': 'kr',
        'apiKey': api_key,
        'pageSize': page_size
    }
    if category:
        params['category'] = category
    
    response = requests.get(url, params=params)
    data = response.json()
    articles = data['articles']
    # YouTube 소스를 제외하고 필터링
    filtered_articles = [article for article in articles if article['source']['name'] != 'YouTube']
    # 최신 10개 기사만 남김
    df = pd.DataFrame(filtered_articles).head(10)
    return df

def load_to_redshift(schema, categories):
    redshift_hook = PostgresHook(postgres_conn_id='redshift_morning_slack')  # Airflow connection ID 사용
    conn = redshift_hook.get_conn()
    cur = conn.cursor()
    
    for category in categories:
        df = fetch_news(category, page_size=20)
        table_name = 'news'
        if category:
            table_name = f'news_{category}'
        table_name = f"{schema}.{table_name}"
        
        # Drop table if it exists
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        cur.execute(drop_table_query)
        
        # Create table
        create_table_query = f"""
        CREATE TABLE {table_name} (
            source VARCHAR(256),
            author VARCHAR(256),
            title VARCHAR(512),
            description VARCHAR(8192),
            url VARCHAR(1024),
            url_to_image VARCHAR(1024),
            published_at TIMESTAMP,
            content VARCHAR(8192)
        );
        """
        cur.execute(create_table_query)
        
        # Insert data into Redshift
        for index, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (source, author, title, description, url, published_at, content)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            cur.execute(insert_query, (row['source']['name'], row['author'], row['title'], row['description'], row['url'], row['publishedAt'], row['content']))
    
    conn.commit()
    cur.close()
    conn.close()

dag = DAG(
    dag_id='news_to_redshift_combined',
    start_date=datetime(2024, 6, 10),
    schedule_interval='0 3 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=3),
    }
)

categories = [None, 'business', 'entertainment', 'health', 'science', 'sports', 'technology']
schema = 'morningslack'  # 스키마 지정.

fetch_and_load_task = PythonOperator(
    task_id='fetch_and_load_news',
    python_callable=load_to_redshift,
    op_args=[schema, categories],
    dag=dag
)
