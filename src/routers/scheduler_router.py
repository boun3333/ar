# main.py
import os
import socket
import time
from datetime import datetime

from fastapi import FastAPI, APIRouter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.pool import  ProcessPoolExecutor
from apscheduler.triggers.date import DateTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.triggers.cron import CronTrigger

from client import AESClient, ESQuery
from dto import TutorManualReportListDto
from data import run_batch
from utils.mapping import result_mapping
from utils.logger import get_logger
from utils.config import SCHEDULER_INDEX, SCHEDULER_CRON, RESULT_INDEX, SINCE_INDEX

logger = get_logger("stdout", __name__)

router = APIRouter(tags=['스케줄러'])

# -----------------------------------------------------------------------------------------
# 환경 설정
# -----------------------------------------------------------------------------------------
TIMEZONE = os.getenv("TZ_REGION", "Asia/Seoul")          # APScheduler 타임존
# 문서 ID는 "host-pid" 조합으로 고정 (프로세스마다 유일)
HOSTNAME = socket.gethostname()
PID = os.getpid()
DOC_ID = f"{HOSTNAME}-{PID}"

def _now_ms() -> int:
    return int(time.time() * 1000)

def _build_trigger(expr: str) -> CronTrigger:
    # 초 없이 5필드 cron만 사용
    minute, hour, day, month, dow = expr.split()
    return CronTrigger(minute=minute, hour=hour, day=day, month=month, day_of_week=dow, timezone=TIMEZONE)

def batch_entrypoint_sync(rptc_ids: list[str] | None = None):
    import asyncio
    asyncio.run(preprocess_batch(rptc_ids))

async def preprocess_batch(rptc_ids: list[str] | None = None):
    logger.info(f"[BATCH] START PID={PID}")
    await run_batch(rptc_ids=rptc_ids)
    logger.info(f"[BATCH] STOP  PID={PID}")

# ======  리더 선정 로직 ======
async def start_scheduler_if_leader(app: FastAPI) -> bool:
    try:
        if SCHEDULER_CRON:
            logger.info(f"[SCHEDULER] start hostname={HOSTNAME},pid={PID}")
            client = AESClient()
            time.sleep(10)
            # (1) insert
            await client.insert(
                index=SCHEDULER_INDEX,
                id=DOC_ID,  # host-pid 조합으로 유일
                document={
                    "host": HOSTNAME,
                    "pid": PID,
                    "created_at": _now_ms(),
                }
            )

            query = ESQuery()
            query.set_sort(field="created_at", order="asc")
            query.set_size(1)

            # (3) 오름차순으로 첫 문서가 '나'인지 체크
            resp = await client.search(index=SCHEDULER_INDEX, query=query, _all=True)
            hits = resp
            first_prs = bool(hits) and hits[0]["_id"] == DOC_ID

            if not first_prs:
                logger.info(f"[SCHEDULER] NOT leader (host={HOSTNAME}, pid={PID})")
                return False

            # 스케줄러 등록
            scheduler = AsyncIOScheduler(
                jobstores={"default": MemoryJobStore()},
                executors={"processpool": ProcessPoolExecutor(max_workers=2)},
                job_defaults={
                    "coalesce": True,
                    "max_instances": 1,
                    "misfire_grace_time": 300,
                },
                timezone=TIMEZONE,
            )
            scheduler.add_job(batch_entrypoint_sync,
                              trigger=_build_trigger(SCHEDULER_CRON),
                              id="preprocess_batch",
                              replace_existing=True,
                              executor="processpool")

            scheduler.start()
            logger.info(f"[SCHEDULER] started as LEADER (cron='{SCHEDULER_CRON}', tz={TIMEZONE})")

            return True
        else:
            return False
    except Exception:
        logger.exception("[SCHEDULER] start failed")
        return False
    finally:
        if SCHEDULER_CRON:
            await client._close_connection()

def shutdown_scheduler(app: FastAPI):
    """
    최소 정리:
      - 스케줄러만 정상 종료
      - (원하시면 여기서 ES 문서 삭제도 추가 가능)
    """
    try:
        scheduler: AsyncIOScheduler = getattr(app.state, "scheduler", None)
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("[SCHEDULER] shutdown complete")
    except Exception:
        logger.exception("[SCHEDULER] shutdown failed")

async def scheduler_init_app():
    client = AESClient()
    logger.info(f"[INIT] 메인프로세스 스케줄러 초기화")
    if not await client.exists_index(index=RESULT_INDEX):
        iscretae = await client.create(index=RESULT_INDEX, mapping=result_mapping)
        logger.info(f"[INIT] 메인프로세스 {RESULT_INDEX} 초기화 완료 > {iscretae}")

    if not await client.exists_index(index=SINCE_INDEX):
        iscretae = await client.create(index=SINCE_INDEX)
        logger.info(f"[INIT] 메인프로세스 {SINCE_INDEX} 초기화 완료 > {iscretae}")


    if await client.exists_index(index=SCHEDULER_INDEX):
        query = {"match_all": {}}
        deleted = await client.delete_by_query(index=SCHEDULER_INDEX, query=query)
        logger.info(f"[INIT] 메인프로세스 스케줄러 인덱스 초기화 완료")
    else:
        logger.info(f"[INIT] 스케줄러 인덱스 없음")

    await client._close_connection()
    return True

# 보고서 내용 받고 수동 실행
@router.post("/ai/preprocess/batch")
async def manual_preprocess(body:TutorManualReportListDto):
    run_at = datetime.now()
    job_id = f"manual_preprocess_{int(run_at.timestamp())}"

    if not body.rptc_list:
        return {
            "status": "002",
            "message": "보고서 데이터를 입력해주세요.",
        }

    # 스케줄러 등록
    scheduler = AsyncIOScheduler(
        jobstores={"default": MemoryJobStore()},
        executors={"processpool": ProcessPoolExecutor(max_workers=2)},
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 300,
        },
        timezone=TIMEZONE,
    )

    # body에 담긴 리스트를 그대로 넘김 (예: body.rptc_ids: list[str])
    scheduler.add_job(
        batch_entrypoint_sync,
        trigger=DateTrigger(run_date=datetime.now()),
        id=job_id,
        kwargs={"rptc_ids": body.rptc_list},
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        executor="processpool"
    )

    scheduler.start()

    logger.info(f"[MANUAL]수동 전처리 스케줄러 등록 (job_id={job_id}, size={len(body.rptc_list)}, at={run_at.isoformat()})")
    return {
        "status": "001",
        "message": "수동 전처리 작업이 스케줄러에 등록되었습니다.",
        "job_id": job_id,
        "scheduled_for": run_at.isoformat(),
        "count": len(body.rptc_list),
    }


