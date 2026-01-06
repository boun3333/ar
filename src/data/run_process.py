import asyncio
import os
import time
from datetime import datetime
import pandas as pd
import json
from client import AESClient
from data import DataBuilder, preprocessing_rptc
from model import Clova
from utils.logger import get_logger
from utils.config import RESULT_INDEX, ERROR_INDEX

class Process:
    def __init__(self, rptc_ids=None):
        self.rptc_ids = rptc_ids
        self.builder = DataBuilder(rptc_ids=rptc_ids)
        self.clova = Clova()
        self.logger = get_logger("stdout")
        self.logger.info(f"[BATCH] START RUN BATCH - AI TUTOR")
        self.client = AESClient()

    def format_duration(self, seconds: float) -> str:
        minutes, sec = divmod(seconds, 60)
        return f"{int(minutes)}분 {sec:.3f}초"

    async def close_connection(self):
        await self.client._close_connection()

    # 임시
    def get_tutor_eval_ids(self, preprocessed_data):
        target_rptcs = {}
        keys = list(preprocessed_data.keys())
        target_ids = keys[:100]
        for rptc_id in target_ids:
            content = preprocessed_data.get(rptc_id)
            if not content:
                continue

            try:
                parsed_json = json.loads(content["json_str"])
                target_rptcs[rptc_id] = parsed_json
            except json.JSONDecodeError:
                continue
        return target_rptcs

    async def run_batch_process(self):

        total_start = time.perf_counter()
        step_start = time.perf_counter()
        since = None
        self.logger.debug("[BATCH] ========== 1단계 : 데이터 조회 ==========")
        try:
            rptc_info_df = await self.builder.get_rptc_info()
            # since 추가 예정
            if rptc_info_df.empty:
                self.logger.info("[BATCH] Report Not Found")
                return False

            rptc_layout_df = await self.builder.get_rptc_layout()
            analysis_df = await self.builder.get_rptc_analysis()
            #login_df = self.builder.get_user_info()

        except Exception as e:
            self.logger.error(f"[BATCH-ERROR]-[STEP1] ERROR : {e}")
            return False

        self.logger.debug(f"[BATCH-TIME] 1단계 완료 (소요시간: {self.format_duration(time.perf_counter() - step_start)})")

        step_start = time.perf_counter()
        self.logger.debug("[BATCH] ========== 2단계 : 데이터 병합 ==========")
        try:
            rptc_layout_df['qa_structure'] = rptc_layout_df.apply(self.builder.build_question_structure, axis=1)
            qa_dict = rptc_layout_df.groupby('RPTC_ID')['qa_structure'].apply(list).to_dict()

            analysis_grouped = self.builder.build_analysis_grouped(analysis_df=analysis_df)
            # rptc_info랑 analysis_df는 무조건 있어야 하는가? 조건처리 필요
            expanded_rows = self.builder.build_rptc_init(qa_dict=qa_dict, rptc_info_df=rptc_info_df, rptc_layout_df=rptc_layout_df, analysis_grouped=analysis_grouped)
            expanded_df = pd.DataFrame(expanded_rows)
            merge_df = pd.merge(rptc_info_df, expanded_df, on='RPTC_ID', how='left')
            base_cols = list(rptc_info_df.columns)
            ordered_cols = self.builder.build_culumn_sorting(base_cols=base_cols)
            merge_df = merge_df[ordered_cols]
            merge_df.replace({r'\r\n': ' ', r'\n': ' '}, regex=True, inplace=True)
        except Exception as e:
            self.logger.error(f"[BATCH-ERROR]-[STEP2] ERROR : {e}")
            return False
        self.logger.debug(f"[BATCH-TIME] 2단계 완료 (소요시간: {self.format_duration(time.perf_counter() - step_start)})")

        step_start = time.perf_counter()
        self.logger.debug("[BATCH] ========== 3단계 : 데이터 전처리 ==========")
        try:
            preprocess_data = preprocessing_rptc(merge_df=merge_df)
            self.logger.debug(f"[BATCH-TIME] 3단계 완료 (소요시간: {self.format_duration(time.perf_counter() - step_start)})")
        except Exception as e:
            self.logger.error(f"[BATCH-ERROR]-[STEP3] ERROR : {e}")
            return False

        step_start = time.perf_counter()
        self.logger.debug("[BATCH] ========== 4단계 : AI 보고서 평가 시작 ==========")
        target_rptcs = self.get_tutor_eval_ids(preprocessed_data=preprocess_data)

        # ====== START: 4단계 for문을 병렬 처리로 변경 ======
        MAX_CONCURRENCY = int(os.getenv("STEP4_CONCURRENCY", "5"))  # CHANGED
        sem = asyncio.Semaphore(MAX_CONCURRENCY)  # CHANGED

        # CHANGED: 개별 보고서(rptc_id) 처리용 코루틴 정의
        async def process_one(rptc_id: str, rptc_item: dict):  # CHANGED
            async with sem:  # CHANGED
                try:
                    self.logger.debug(f"[BATCH] 보고서 아이디 : {rptc_id} 평가 시작")
                    # 이미 분석 완료한 보고서 아이디면 넘어가기 추가 (기존 로직 유지)
                    parsed_json = json.loads(rptc_item['json_str'])

                    result = await asyncio.to_thread(
                        self.clova.run_ai_tutor, rptc_id, parsed_json
                    )

                    self.logger.debug(f"[BATCH] 보고서 아이디 : {rptc_id} 평가 종료")
                    #self.logger.debug(f"[BATCH] 보고서 아이디 : {rptc_id} 평가 결과 : {json.dumps(result, indent=2)}")
                    result.__setitem__('mdfcn_dt', rptc_item["MDFCN_DT"])  # CHANGED
                    isinsert = await self.client.insert(index=RESULT_INDEX, id=rptc_id, document=result)
                    if isinsert:
                        self.logger.debug(f"[BATCH] 보고서 아이디 : {rptc_id} 평가 저장 완료")
                except Exception as e:  # CHANGED
                    # 기존 오류 로깅/저장 로직을 유지합니다.
                    self.logger.error(f"[BATCH-ERROR]-[STEP4] ERROR : {e}", exc_info=True)  #
                    error_doc = {
                        "rptc_id": rptc_id,
                        "error": str(e),
                        #"rptc_item": rptc_item['json_str'],
                        "created_dt": datetime.now().isoformat(),
                    }
                    _id = rptc_id + error_doc['created_dt']  # CHANGED
                    await self.client.insert(index=ERROR_INDEX, id=_id, document=error_doc)  # CHANGED

        # CHANGED: 태스크 생성
        tasks = [asyncio.create_task(process_one(rid, item)) for rid, item in preprocess_data.items()]  # CHANGED

        # CHANGED: 완료되는 순서대로 기다리며 예외 전파/로깅은 process_one 내부에서 처리
        for fut in asyncio.as_completed(tasks):
            try:# CHANGED
                await fut  # CHANGED
            except Exception as e:
                self.logger.error("[BATCH-ERROR]-[STEP4] Task crashed: %s", e, exc_info=True)
                # 원하면 여기서도 ERROR_INDEX에 남길 수 있음(간단 요약 형태로)
                # await self.client.insert(index=ERROR_INDEX, id=..., document={...})
                continue
        # ====== CHANGED END ======

        self.logger.debug(f"[BATCH-TIME] 4단계 완료 (소요시간: {self.format_duration(time.perf_counter() - step_start)})")

        # 총 실행 시간
        self.logger.debug(f"[BATCH-TIME] ===== 전체 완료 (총 소요시간: {self.format_duration(time.perf_counter() - total_start)}) =====")

async def run_batch(rptc_ids: list[str] | None = None):
    process = Process(rptc_ids=rptc_ids)
    await process.run_batch_process()
    await process.close_connection()
