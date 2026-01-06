import os
import socket
import uvicorn
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from utils.logger import get_logger
from utils.config import APP_HOST, APP_PORT, APP_WORKER
from routers import search_router, scheduler_router
from routers.scheduler_router import start_scheduler_if_leader, shutdown_scheduler, scheduler_init_app

@asynccontextmanager
async def lifespan(app: FastAPI):
    started = await start_scheduler_if_leader(app)  # 리더면 스케줄러 가동
    try:
        yield
    finally:
        if started:
            shutdown_scheduler(app)


app = FastAPI(lifespan=lifespan)
logger = get_logger("stdout")

app.include_router(search_router.router)
app.include_router(scheduler_router.router)

if __name__ == "__main__":
    asyncio.run(scheduler_init_app())
    uvicorn.run("app:app", host=APP_HOST, port=APP_PORT, workers=APP_WORKER, log_config=None)