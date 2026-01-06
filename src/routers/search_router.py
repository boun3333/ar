from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from data import SearchData
from dto import TutorReportSearchDto
from utils.logger import get_logger
from utils.status_code import response_send

router = APIRouter(tags=['보고서 조회'])
router.logger = get_logger("stdout")
router.search_client = SearchData()

@router.get("/ai/tutor/home")
async def home():
    return {"message": "AI Tutor Home"}

@router.post("/ai/tutor/report")
async def tutor_report(body: TutorReportSearchDto):
    try:
        router.logger.info(f"[SEARCH] {body}")
        res = await router.search_client.get_report_tutor_eval(body=body)

        return JSONResponse(
            content=res,
            status_code=200
        )
    except HTTPException as e:
        router.logger.error(f"[SEARCH-ERROR] - {e}")
        return JSONResponse(
            content=response_send('999'),
            status_code=400
        )