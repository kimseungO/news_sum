# Python 3.10 버전의 경량 Debian 기반 이미지를 사용합니다.
FROM python:3.10-slim-buster

# 컨테이너 내 작업 디렉토리를 설정합니다.
WORKDIR /app

# mysql-connector-python 컴파일에 필요한 시스템 종속성을 설치합니다.
# build-essential, gcc는 컴파일에 필요하고, default-libmysqlclient-dev는 MySQL 클라이언트 라이브러리입니다.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# requirements_py310.txt 파일을 컨테이너에 복사하고 Python 종속성을 설치합니다.
# --no-cache-dir 옵션은 설치 후 pip 캐시를 삭제하여 이미지 크기를 줄입니다.
COPY requirements_py310.txt .
RUN pip install --no-cache-dir -r requirements_py310.txt

# .env 파일과 AIapi.py 스크립트를 컨테이너의 작업 디렉토리로 복사합니다.
# .env 파일은 보안상 Docker Compose의 environment 섹션을 통해 전달하는 것이 더 안전할 수 있습니다.
COPY .env .
COPY AIapi.py .

# 데이터 파일이 저장될 디렉토리를 생성합니다.
# 이 디렉토리는 docker-compose.yml에서 호스트의 'data' 디렉토리와 마운트될 것입니다.
RUN mkdir -p /app/data

# 컨테이너가 시작될 때 실행될 기본 명령을 정의합니다.
# AIapi.py는 news_cluster.py가 생성한 news_preproc.xlsx 파일을 /app/data에서 읽을 것입니다.
# 이 컨테이너는 docker-compose.yml에서 app_py38 서비스가 성공적으로 완료될 때까지 기다리도록 설정됩니다.
CMD ["python3", "AIapi.py"]
