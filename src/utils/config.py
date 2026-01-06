import os
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")


APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = int(os.getenv("APP_PORT", 8000))
APP_WORKER = int(os.getenv("APP_WORKER", 1))
LOG_DIR = os.getenv("LOG_DIR", "../logs")
LOG_CONFIG_PATH = os.getenv("LOG_CONFIG_PATH", "./logging.yaml")

REPO_HOST=os.getenv("REPO_HOST", "http://localhost:9200")
REPO_USER=os.getenv("REPO_USER", "intube")
REPO_PASS=os.getenv("REPO_PASS", "Tubeai1!")

SCHEDULER_INDEX=os.getenv("SCHEDULER_INDEX", "schduler-tutor-ai")
SCHEDULER_CRON=os.getenv("SCHEDULER_CRON", "")
RESULT_INDEX=os.getenv("RESULT_INDEX", "science-tutor-result")
SINCE_INDEX=os.getenv("SINCE_INDEX", "science-tutor-since")
ERROR_INDEX=os.getenv("ERROR_INDEX", "science-tutor-error")

CLOVAX_API_KEY=os.getenv("CLOVAX_API_KEY", "")
CLOVAX_URL=os.getenv("CLOVAX_URL", "https://clovastudio.stream.ntruss.com/testapp/v3/chat-completions")
CLOVAX_TOKEN_URL=os.getenv("CLOVAX_TOKEN_URL", "https://clovastudio.stream.ntruss.com/v3/api-tools/chat-tokenize")
