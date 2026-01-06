FROM python:3.11-slim
 
# 앱 기준 디렉토리로 이동 (src 내에 app.py, config.py 등이 있음)
WORKDIR /src

# copy code: host src/ -> container /src
COPY src/ .
COPY logging.yaml ./
 
# 필수 패키지만 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*
  
RUN mkdir -pv /logs
 
# 전체 proxy 디렉토리 복사 (requirements.txt 포함)
COPY requirements.txt /src/requirements.txt
 
# 의존성 설치는 requirements.txt가 있는 상위 디렉토리에서
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 
 
# .env.example 파일 복사 (선택)
COPY .env.example /src/.env
 
# FastAPI 실행
CMD ["python", "app.py"]
