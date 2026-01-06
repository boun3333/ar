from elasticsearch import Elasticsearch
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")

REPO_HOST=os.getenv("REPO_HOST", "http://localhost:9200")
REPO_USER=os.getenv("REPO_USER", "intube")
REPO_PASS=os.getenv("REPO_PASS", "Tubeai1!")

RESULT_INDEX=os.getenv("RESULT_INDEX", "science-tutor-result")
HOSTS = REPO_HOST.split(",")

client = Elasticsearch(hosts=HOSTS, verify_certs=False, basic_auth=(REPO_USER, REPO_PASS))

client.indices.delete(index=RESULT_INDEX)


