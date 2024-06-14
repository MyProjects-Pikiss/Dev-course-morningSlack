import json
import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.models.blocks import SectionBlock, ActionsBlock, ButtonElement, Option, DividerBlock
import psycopg2
from datetime import datetime, timedelta


# 설정 파일 읽기
with open("/opt/airflow/slack_info.json") as f:
    config = json.load(f)

# 특정 채널 ID 설정
TARGET_CHANNEL_ID = 'C075U6B360P'

# DB 접속정보 [slack_info.json]
dbname = config['DB_NAME']
user = config['USER']
password = config['PASSWORD']
host = config['HOST']
port = config["PORT"]
schema = config["SCHEMA"]

app = App(token=config["SLACK_BOT_TOKEN"])

# 사용자의 상태를 저장할 딕셔너리
user_states = {}

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# DB 연결 테스트
def connect_to_redshift(dbname,user,password,host,port,schema):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
      
    except Exception as e:
        print(f"Error : {e}")
        raise
        
# 유저 상태 초기화.
def reset_user_state(user_id):
    if user_id in user_states:
        del user_states[user_id]

# 초기 메세지와 버튼 초기화
def display_initial_options(say):
    blocks = [
        SectionBlock(
            block_id="section1",
            text="무엇을 도와드릴까요?"
        ),
        ActionsBlock(
            block_id="actions1",
            elements=[
                ButtonElement(
                    text="날씨를 알려줘",
                    action_id="button_weather",
                    value="weather"
                ),
                ButtonElement(
                    text="뉴스를 보여줘",
                    action_id="button_news",
                    value="news"
                ),
                ButtonElement(
                    text="소요시간을 알려줘",
                    action_id="button_duration",
                    value="duration"
                )
            ]
        )
    ]
    say(blocks=blocks)

# 지역에 따른 날씨를 가져오는 함수
# 지역은 user_location['address'] 의 첫 공백과 두번째 공백 사이에 있는 값을 가져와 테이블 간 join 을 진행
# 조회기간은 당시 서버시간 ~ 서버시간+4 까지 조회
def get_weather_from_db(location):
    conn = connect_to_redshift(dbname, user, password, host, port, schema)
    current_date_time = datetime.now()
    date = current_date_time.date()
    start_time = current_date_time.time()
    end_time = (current_date_time + timedelta(hours=4)).time()
    try:
        with conn.cursor() as cur:
            query = f"""
            WITH target_location AS (
                SELECT id, address
                FROM {schema}.user_location
                WHERE address LIKE '%{location}%'
            ),
            weather_info AS (
                SELECT t2.*
                FROM {schema}.hourly_local_forecast t2
                JOIN target_location tl ON t2.location = tl.id
                WHERE DATE(t2.fcst_timestamp) = '{date}'
            ),
            dust_info AS (
                SELECT t3.*
                FROM {schema}.fine_dust t3
                JOIN target_location tl 
                ON t3.station_name = TRIM(SUBSTRING(tl.address FROM POSITION(' ' IN tl.address) + 1 
                            FOR POSITION(' ' IN SUBSTRING(tl.address FROM POSITION(' ' IN tl.address) + 1)) - 1))
                WHERE DATE(t3.datatime) = '{date}'
            )
            SELECT 
                tl.address,
                wi.fcst_timestamp,
                wi.tmp,
                wi.sky,
                wi.pcp,
                wi.reh,
                wi.pty,
                di.pm10grade,
                di.pm25grade
            FROM 
                target_location tl
            JOIN 
                weather_info wi ON wi.location = tl.id
            LEFT JOIN 
                dust_info di ON di.station_name = TRIM(SUBSTRING(tl.address FROM POSITION(' ' IN tl.address) + 1 
                                FOR POSITION(' ' IN SUBSTRING(tl.address FROM POSITION(' ' IN tl.address) + 1)) - 1))
            AND wi.fcst_timestamp = di.datatime
            WHERE wi.fcst_timestamp >= '{date} {start_time}' AND wi.fcst_timestamp <= '{date} {end_time}'
            ORDER BY
                wi.fcst_timestamp;
            """
            cur.execute(query)
            results = cur.fetchall()
            if results:
                weather_report = "\n".join([f"{row[1].strftime('%H:%M')}시: 기온 {row[2]}°C, 강수량 {row[4]}, 습도 {row[5]}%, 강수확률 {row[6]}(%), 미세먼지 {row[7] if row[7] else '정보 없음'}, 초미세먼지 {row[8] if row[8] else '정보 없음'}" for row in results])
                return weather_report
            else:
                return None
    except Exception as e:
        logger.error(f'Error fetching weather data: {e}')
        return None
    finally:
        conn.close()

# 뉴스 정보를 가져오는 함수
# published_at 를 기준으로 최신 뉴스 3개만 가져와 보여주도록함.
def get_news(category):
    # 카테고리별 최신뉴스 3개를 가져와서 보여준다.
    category_map = {
        "비즈니스": "business",
        "테크": "technology",
        "연예": "entertainment",
        "스포츠": "sports",
        "건강" : "health",
        "과학" : "science"
    }
    category_english = category_map.get(category)
    if not category_english:
        logger.error(f'Invalid category: {category}')
        return None
    
    conn = connect_to_redshift(dbname, user, password, host, port, schema)
    query = f"""SELECT title, description, url FROM news_{category_english} ORDER BY published_at DESC LIMIT 3"""
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            news_data = [{"title": row[0], "url": row[2]} for row in result]
    except Exception as e:
        logger.error(f'Error fetching news data: {e}')
        return None
    finally:
        conn.close()

    return news_data

# 출발지 ~ 도착지 소요시간을 가져오는 함수
# duration_info 테이블을 참고하여 특정 유저가 사용하는 상황을 가정
# 특정 유저의 출발지와 도착지가 저장되어 있고 그에 따라 소요시간을 조회
def get_duration_from_db():
    conn = connect_to_redshift(dbname, user, password, host, port, schema)
    query = f"""SELECT * FROM {schema}.duration_info LIMIT 1"""
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            if row:
                navi = {"origin": row[1], "destination": row[2], "duration": row[3]}
                return navi
            else:
                return None
    except Exception as e:
        logger.error(f'Error fetching duration data: {e}')
        return None
    finally:
        conn.close()

# 이벤트 처리 로직을 공통 함수로 분리
def handle_event(event, say, logger):
    user_id = event['user']
    channel_id = event['channel']
    text = event['text']

    logger.info(f"Received event: {event}")

    if channel_id != TARGET_CHANNEL_ID:
        return

    if user_id not in user_states:
        display_initial_options(say)
        user_states[user_id] = 'waiting_for_selection'
    elif user_states[user_id] == 'waiting_for_location':
        location = text.strip()
        weather_info = get_weather_from_db(location)
        if weather_info:
            say(f"{location}의 날씨는 {weather_info}입니다.")
        else:
            say(f"{location}의 날씨 정보를 가져올 수 없습니다.")
        reset_user_state(user_id)


# app_mention 이벤트 핸들러
@app.event("app_mention")
def handle_app_mention_events(body, say, logger):
    handle_event(body['event'], say, logger)

# message 이벤트 핸들러
@app.event("message")
def handle_message_events(body, say, logger):
    event = body['event']
    # @슬랙봇 과 같이 멘션을 하지 않을 때 처리 해야하는 경우
    if 'subtype' not in event or event['subtype'] != 'bot_message':
        handle_event(event, say, logger)

# 아래는 각 버튼에 따른 이벤트, user_state 지정
@app.action("button_weather")
def handle_button_weather(ack, body, say):
    ack()
    user_id = body['user']['id']
    say("어느 지역의 날씨를 알려드릴까요?")
    user_states[user_id] = 'waiting_for_location'

@app.action("button_news")
def handle_button_news(ack, body, say):
    ack()
    user_id = body['user']['id']
    
    blocks = [
        SectionBlock(
            block_id="section2",
            text="어떤 종류의 뉴스를 보여드릴까요?"
        ),
        ActionsBlock(
            block_id="actions2",
            elements=[
                ButtonElement(
                    text="비즈니스",
                    action_id="news_business",
                    value="비즈니스"
                ),
                ButtonElement(
                    text="건강",
                    action_id="news_health",
                    value="건강"
                ),
                ButtonElement(
                    text="연예",
                    action_id="news_entertainment",
                    value="연예"
                ),
                ButtonElement(
                    text="스포츠",
                    action_id="news_sports",
                    value="스포츠"
                ),
                ButtonElement(
                    text="테크",
                    action_id="news_technology",
                    value="테크"
                ),
                ButtonElement(
                    text="과학",
                    action_id="news_science",
                    value="과학"
                )
            ]
        )
    ]
    say(blocks=blocks)
    user_states[user_id] = 'waiting_for_news_category'

@app.action("news_business")
@app.action("news_technology")
@app.action("news_entertainment")
@app.action("news_sports")
@app.action("news_science")
@app.action("news_health")
def handle_news_category_selection(ack, body, say):
    ack()
    user_id = body['user']['id']
    action_id = body['actions'][0]['action_id']
    category_map = {
        "news_business": "비즈니스",
        "news_technology": "테크",
        "news_entertainment": "연예",
        "news_sports": "스포츠",
        "news_health": "건강",
        "news_science": "과학"
    }
    category = category_map.get(action_id, "")
    if category:
        news_data = get_news(category)
        if news_data:
            blocks = [
                SectionBlock(block_id="section3", text=f"오늘의 {category} 뉴스입니다:"),
                DividerBlock()
            ]
            for news in news_data:
                blocks.append(SectionBlock(text=f"<{news['url']}|{news['title']}>"))
            say(blocks=blocks)
        else:
            say(f"{category} 뉴스 데이터를 가져오는 데 문제가 발생했습니다.")
        reset_user_state(user_id)
    else:
        say("잘못된 카테고리 선택입니다. 다시 시도해주세요.")

@app.action("button_duration")
def handle_button_duration(ack, body, say):
    ack()
    duration_info = get_duration_from_db()
    if duration_info:
        say(f"{duration_info['origin']}에서 {duration_info['destination']}까지의 소요시간은 {duration_info['duration']/60 :.2f}분 소요됩니다.")
    else:
        say("소요시간 정보를 가져올 수 없습니다.")

if __name__ == "__main__":
    handler = SocketModeHandler(app, config["SLACK_APP_TOKEN"])
    handler.start()
