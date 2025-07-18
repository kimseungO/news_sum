import os
from dotenv import load_dotenv
import google.generativeai as genai
import json
import pandas as pd
from datetime import datetime
import mysql.connector # MySQL 연결을 위한 라이브러리
import time

# .env 파일 로드
load_dotenv()

# Gemini API 키 설정
try:
    genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
except KeyError:
    print("오류: 'GOOGLE_API_KEY' 환경 변수가 설정되지 않았습니다.")
    print("API 키를 설정하거나 코드 내에서 genai.configure(api_key='YOUR_API_KEY')를 사용하세요.")
    exit()

# Gemini 모델 설정
MODEL_NAME = 'gemini-2.0-flash' # 또는 'gemini-1.5-pro' 등으로 변경 가능
model = genai.GenerativeModel(MODEL_NAME)

# 데이터 로드
try:
    data = pd.read_excel("/app/data/news_preproc.xlsx")
    data = data.sort_values(by="cluster2nd").reset_index(drop=True)
except FileNotFoundError:
    print("오류: 'news_preproc.xlsx' 파일을 찾을 수 없습니다. 파일 경로를 확인하세요.")
    exit()

# DB 연결 설정 (루프 시작 전 한 번만 연결)
try:
    conn = mysql.connector.connect(
        host=os.environ.get("DB_HOST", "localhost"), # 환경 변수 없으면 'localhost' 기본값
        user=os.environ.get("DB_USER", "root"),       # 환경 변수 없으면 'root' 기본값
        password=os.environ.get("DB_PASSWORD", ""),   # 환경 변수 없으면 빈 문자열 기본값
        database=os.environ.get("DB_NAME", "test_db"), # 환경 변수 없으면 'test_db' 기본값
    )
    cursor = conn.cursor()

    # news_sum 테이블 생성 (없을 경우)
    # keyword 컬럼은 VARCHAR(255) 또는 TEXT로 충분히 길게 설정해야 합니다.
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS news_sum (
        topic_id INT PRIMARY KEY,       -- cluster2nd
        topic_title VARCHAR(255),
        topic_content TEXT,
        new_cnt INT,
        sum_date TIMESTAMP,
        keyword VARCHAR(500) -- 키워드 목록을 저장하기 위해 충분히 길게 설정 (예: VARCHAR(500) 또는 TEXT)
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    print("✅ 데이터베이스 연결 및 'news_sum' 테이블 확인/생성 완료.")

except mysql.connector.Error as err:
    print(f"데이터베이스 연결 또는 테이블 생성 오류: {err}")
    exit()

# 고유한 클러스터 ID 목록 가져오기
cluster_ids = sorted(data['cluster2nd'].dropna().unique())

# 각 클러스터 처리
for cluster_id in cluster_ids:
    # cluster2nd가 0인 경우는 스킵
    if cluster_id == 0:
        continue
    # 시험용으로 cluster_id가 11 이상인 경우 중단 (원하시면 이 조건문 제거)
    #if 11 <= cluster_id:
    #    print(f"[{cluster_id}] 클러스터 ID 11 이상이므로 처리 중단.")
    #    break
    time.sleep(3)
    target_df = data[data['cluster2nd'] == cluster_id]
    news_contents = target_df['contents'].dropna().tolist()

    if not news_contents:
        print(f"[{cluster_id}] 스킵됨: 'contents'가 없습니다.")
        continue

    # 뉴스 본문들을 하나의 문자열로 결합
    joined_contents = "\n\n".join(news_contents)

    # Gemini API 프롬프트 구성 (한국어 버전)
    # 키워드 생성 지침을 추가하고, 출력 형식에 'keyword'를 포함하도록 명확히 지시
    prompt_text = f"""
    당신은 숙련된 뉴스 요약 전문가입니다. 당신의 임무는 제공된 여러 뉴스 본문을 통해 동일한 주제의 뉴스 기사들을 분석하고, 전체 주제를 가장 잘 나타내는 간결한 **제목(title)** 하나와 모든 기사의 주요 내용을 포괄하는 종합적인 **요약 내용(sum_contents)**을 추출하고, 마지막으로 모든 기사의 핵심을 나타내는 **키워드 목록(keyword)**을 추출하는 것입니다.

    ---
    **입력:**

    다음은 뉴스 기사 본문들입니다:
    {joined_contents}

    ---
    **출력 형식:**

    다음 키를 포함하는 JSON 형식으로 결과를 제공해 주세요:
    -   `title`: 제공된 모든 기사의 핵심을 담는 간결하고 정확한 제목.
    -   `sum_contents`: 모든 기사의 핵심 정보, 주요 사건, 중요한 시사점을 종합하는 포괄적인 요약 (최소 3문장, 최대 7문장). 요약은 응집력이 있어야 하며 중복된 정보를 피해야 합니다.
    -   `keyword`: 뉴스 본문에서 추출된 핵심 키워드 목록. 각 키워드는 개별 문자열로 구성된 배열이어야 합니다 (예: ["키워드1", "키워드2", "키워드3"]). 최소 5개, 최대 15개의 키워드를 포함해야 합니다.

    ---
    **지침:**

    1.  제공된 뉴스 본문에서 핵심 주제와 가장 중요한 세부 사항을 식별하세요.
    2.  공통된 주제를 반영하는 단일하고 포괄적인 `title`을 생성하세요.
    3.  모든 기사의 정보를 통합하여 주요 측면과 전반적인 전개 상황을 강조하는 `sum_contents`를 작성하세요.
    4.  `sum_contents`는 최소 3문장, 최대 7문장으로 구성되어야 하며, 응집력이 있고 자연스럽게 흐르도록 하세요.
    5.  `keyword`는 제공된 뉴스 본문에서 핵심 주제와 가장 중요한 세부 사항을 포함하는 개별 키워드들을 배열 형태로 추출하세요.
    """

    print(f"\n[{cluster_id}] Gemini API에 요약 요청 중...")

    try:
        # Gemini API 호출 및 JSON 응답 스키마 지정
        response = model.generate_content(
            prompt_text,
            generation_config={
                "response_mime_type": "application/json",
                "response_schema": {
                    "type": "OBJECT",
                    "properties": {
                        "title": {"type": "STRING"},
                        "sum_contents": {"type": "STRING"},
                        "keyword": { # 키워드를 문자열 배열로 정의
                            "type": "ARRAY",
                            "items": {"type": "STRING"}
                        }
                    },
                    "required": ["title", "sum_contents", "keyword"] # keyword도 필수 필드로 지정
                }
            }
        )
        
        # 모델 응답 텍스트를 가져오기 전에 응답이 유효한지 확인
        if not hasattr(response, 'text') or not response.text:
            print(f"[{cluster_id}] 오류: Gemini 모델에서 유효한 텍스트 응답을 받지 못했습니다.")
            print(f"모델 응답 객체: {response}")
            continue # 다음 클러스터로 넘어감

        # JSON 파싱 시도
        summary_data = json.loads(response.text)

        # 키워드 목록을 쉼표로 구분된 문자열로 변환
        # summary_data.get('keyword', [])는 모델이 keyword를 반환하지 않을 경우 빈 리스트를 반환하여 오류 방지
        keywords_str = ", ".join(summary_data.get('keyword', []))

        print(f"--- [{cluster_id}] 요약 결과 (콘솔) ---")
        print(f"제목: {summary_data.get('title', '제목 없음')}")
        print(f"요약 내용:\n{summary_data.get('sum_contents', '요약 내용을 찾을 수 없습니다.')}")
        print(f"키워드: {keywords_str if keywords_str else '키워드를 찾을 수 없습니다.'}")
        print("-" * 30)

        # 요약 날짜 및 시간 생성
        current_summary_date = datetime.now()

        # 해당 cluster2nd 기사들에 요약 결과를 채워넣음 (DataFrame 업데이트)
        data.loc[data['cluster2nd'] == cluster_id, 'sum_title'] = summary_data.get('title')
        data.loc[data['cluster2nd'] == cluster_id, 'sum_contents'] = summary_data.get('sum_contents')
        data.loc[data['cluster2nd'] == cluster_id, 'keyword'] = keywords_str # 변환된 문자열 저장
        data.loc[data['cluster2nd'] == cluster_id, 'sum_date'] = current_summary_date

        # DB 삽입을 위한 데이터 준비
        topic_id = int(cluster_id) # int로 명시적 변환
        topic_title = summary_data.get('title', '제목 없음')
        topic_content = summary_data.get('sum_contents', '요약 없음')
        sum_date = current_summary_date # datetime 객체로 전달

        # 'counts' 컬럼이 있는지 확인하고, 없으면 target_df의 행 개수를 사용
        if 'counts' in target_df.columns and not target_df['counts'].empty:
            new_cnt = int(target_df['counts'].iloc[0]) # numpy.int64 -> int 변환
        else:
            new_cnt = len(target_df) # 기본값으로 해당 클러스터의 뉴스 기사 수 사용

        # DB에 저장할 키워드는 AI가 생성한 키워드 문자열
        db_keyword = keywords_str

        # news_sum 테이블에 INSERT (각 클러스터마다 실행)
        insert_query = """
        INSERT INTO news_sum (topic_id, topic_title, topic_content, new_cnt, sum_date, keyword)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            topic_title = VALUES(topic_title),
            topic_content = VALUES(topic_content),
            new_cnt = VALUES(new_cnt),
            sum_date = VALUES(sum_date),
            keyword = VALUES(keyword)
        """
        # ON DUPLICATE KEY UPDATE를 추가하여 topic_id가 중복될 경우 업데이트하도록 함
        # 이렇게 하면 스크립트를 여러 번 실행해도 중복 에러가 발생하지 않습니다.

        try:
            cursor.execute(insert_query, (topic_id, topic_title, topic_content, new_cnt, sum_date, db_keyword))
            conn.commit()
            print(f"✅ [{cluster_id}] news_sum 테이블에 요약 결과 INSERT/UPDATE 완료!")
        except mysql.connector.Error as db_err:
            print(f"[{cluster_id}] 데이터베이스 삽입 오류: {db_err}")
            conn.rollback() # 오류 발생 시 롤백

    except json.JSONDecodeError as e:
        print(f"[{cluster_id}] 오류: 모델 응답이 유효한 JSON 형식이 아닙니다. JSON 파싱 오류: {e}")
        print(f"\n파싱 시도된 텍스트:\n{response.text if 'response' in locals() and hasattr(response, 'text') else '응답 없음'}")
    except Exception as e:
        print(f"[{cluster_id}] 예상치 못한 오류 발생: {e}")
        if 'response' in locals() and hasattr(response, 'text') and response.text:
            print(f"\n원시 응답 텍스트 (전체):\n{response.text}")

# 모든 클러스터 처리 후 DataFrame을 Excel 파일로 저장
output_filename = '/app/data/news_summary.xlsx'
data.to_excel(output_filename, index=False)
print(f"\n✅ 모든 요약 결과가 '{output_filename}' 파일로 성공적으로 저장되었습니다.")

# 정리 (루프 종료 후 DB 연결 닫기)
if 'cursor' in locals() and cursor:
    cursor.close()
if 'conn' in locals() and conn:
    conn.close()
print("✅ 데이터베이스 연결 종료.")
