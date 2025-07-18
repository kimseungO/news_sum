import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import logging

# 로깅 설정 (스크립트 실행 상황을 파일 또는 콘솔에 기록)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# .env 파일 로드
# Kubernetes 환경이 아닐 경우에만 .env 파일을 로드합니다.
# 스크립트와 같은 디렉토리에 .env 파일이 있다고 가정합니다.
if os.environ.get("KUBERNETES_SERVICE_HOST") is None:
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        logging.info(".env 파일 로드 완료.")
    else:
        logging.warning(".env 파일이 존재하지 않습니다. 환경 변수가 설정되어 있는지 확인하세요.")

# MySQL 연결 설정 (환경 변수가 없으면 기본값 사용)
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "nsuser") # 이전 오류를 고려하여 nsuser 기본값
DB_PASSWORD = os.environ.get("DB_PASSWORD", "your_db_password") # 실제 비밀번호로 변경 필요
DB_NAME = os.environ.get("DB_NAME", "news_db") # 이전 오류를 고려하여 news_db 기본값

conn = None
cursor = None

try:
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4' # 한글 및 이모지 지원을 위해 utf8mb4 설정
    )
    cursor = conn.cursor()
    logging.info("✅ 데이터베이스 연결 성공!")
except mysql.connector.Error as err:
    logging.error(f"❌ 데이터베이스 연결 오류: {err}")
    # 연결 실패 시 스크립트 종료
    exit(1)

# news_preproc.xlsx 파일 로드
excel_file_path = '/app/data/news_preproc.xlsx' # 실제 파일 경로로 변경 필요
data = pd.DataFrame() # 초기화

try:
    data = pd.read_excel(excel_file_path)
    logging.info(f"✅ '{excel_file_path}' 파일 로드 완료. 총 {len(data)}개 행.")
except FileNotFoundError:
    logging.error(f"❌ 오류: '{excel_file_path}' 파일을 찾을 수 없습니다. 경로를 확인해주세요.")
    # 파일이 없으면 데이터베이스 작업을 진행할 수 없으므로 종료
    if cursor: cursor.close()
    if conn: conn.close()
    exit(1)
except Exception as e:
    logging.error(f"❌ Excel 파일 로드 중 예상치 못한 오류 발생: {e}")
    if cursor: cursor.close()
    if conn: conn.close()
    exit(1)

# 데이터프레임이 비어있는지 확인
if data.empty:
    logging.warning("경고: 로드된 Excel 파일에 데이터가 없습니다. 삽입할 내용이 없습니다.")
    if cursor: cursor.close()
    if conn: conn.close()
    exit(0) # 정상 종료

# news_raw 테이블 생성 (없을 경우)
# 이전에 사용하셨던 테이블 구조를 기반으로 합니다.
# news_preproc.xlsx에 이 모든 컬럼이 있다고 가정합니다.
create_table_sql = """
CREATE TABLE IF NOT EXISTS news_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title TEXT,
    url VARCHAR(767) UNIQUE, -- UNIQUE 인덱스 추가 (TEXT 타입에 직접 인덱스 불가, VARCHAR 길이 제한)
    contents LONGTEXT,
    thumbnail TEXT,
    company VARCHAR(100),
    subject VARCHAR(10),
    upload_date DATETIME,
    cluster2nd INT,        -- 클러스터링 결과 (정수)
    keyword VARCHAR(255),  -- 키워드 (문자열)
    counts INT             -- 관련 뉴스 개수 (정수)
);
"""
try:
    cursor.execute(create_table_sql)
    conn.commit()
    logging.info("✅ 'news_raw' 테이블 확인/생성 완료!")
except mysql.connector.Error as err:
    logging.error(f"❌ 테이블 생성 오류: {err}")
    if cursor: cursor.close()
    if conn: conn.close()
    exit(1)

# INSERT 쿼리 (ON DUPLICATE KEY UPDATE를 사용하여 중복 URL 처리)
insert_sql = """
INSERT INTO news_raw (title, url, contents, thumbnail, company, subject, upload_date, cluster2nd, keyword, counts)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    title = VALUES(title),
    contents = VALUES(contents),
    thumbnail = VALUES(thumbnail),
    company = VALUES(company),
    subject = VALUES(subject),
    upload_date = VALUES(upload_date),
    cluster2nd = VALUES(cluster2nd),
    keyword = VALUES(keyword),
    counts = VALUES(counts)
"""

# 데이터 삽입
inserted_count = 0
updated_count = 0
failed_count = 0

for index, row in data.iterrows():
    try:
        # 데이터프레임의 각 컬럼 값을 가져오고, NaN/NaT 값은 None으로 변환하여 DB에 NULL로 저장되도록 처리
        title = row.get('title', None) if pd.notnull(row.get('title')) else None
        url = row.get('url', None) if pd.notnull(row.get('url')) else None
        contents = row.get('contents', None) if pd.notnull(row.get('contents')) else None
        thumbnail = row.get('thumbnail', None) if pd.notnull(row.get('thumbnail')) else None
        company = row.get('company', None) if pd.notnull(row.get('company')) else None
        subject = row.get('subject', None) if pd.notnull(row.get('subject')) else None
        
        # 날짜 데이터 처리: NaT는 None으로 변환
        upload_date = pd.to_datetime(row.get('upload_date')) if pd.notnull(row.get('upload_date')) else None

        # 숫자 데이터 처리: NaN은 None으로 변환
        cluster2nd = int(row.get('cluster2nd')) if pd.notnull(row.get('cluster2nd')) else None
        keyword = row.get('keyword', None) if pd.notnull(row.get('keyword')) else None
        counts = int(row.get('counts')) if pd.notnull(row.get('counts')) else None

        # URL이 없으면 삽입 시도하지 않음 (UNIQUE 키이므로 필수)
        if url is None:
            logging.warning(f"경고: {index}번째 행에 URL이 없어 삽입을 건너뜁니다.")
            failed_count += 1
            continue

        cursor.execute(insert_sql, (
            title, url, contents, thumbnail, company, subject, upload_date,
            cluster2nd, keyword, counts
        ))
        
        # 삽입 또는 업데이트 여부 확인
        if cursor.rowcount == 1: # 1이면 삽입
            inserted_count += 1
        elif cursor.rowcount == 2: # 2이면 업데이트 (ON DUPLICATE KEY UPDATE)
            updated_count += 1

    except Exception as e:
        logging.error(f"❌ 데이터 삽입/업데이트 실패 (URL: {url if url else 'N/A'}, 행 인덱스: {index}): {e}")
        failed_count += 1
        # 개별 실패 시에도 계속 진행
        continue

conn.commit() # 모든 삽입/업데이트 작업 후 최종 커밋
logging.info(f"✅ DB에 뉴스 데이터 저장 완료! (총 {len(data)}개 시도)")
logging.info(f"➡️ 성공적으로 삽입된 행: {inserted_count}개")
logging.info(f"➡️ 업데이트된 행: {updated_count}개")
logging.info(f"➡️ 실패한 행: {failed_count}개")

# 데이터베이스 연결 종료
if cursor:
    cursor.close()
if conn:
    conn.close()
logging.info("✅ 데이터베이스 연결 종료.")
