import os
import re
import json
import time
from datetime import datetime
import requests
from PIL import Image
import io
import base64
from io import BytesIO
import uuid
from pathlib import Path
from utils.config import CLOVAX_API_KEY, CLOVAX_URL, CLOVAX_TOKEN_URL
from utils.logger import get_logger

class Clova:
    def __init__(self, model_name="HCX-005", max_tokens=500, temperature=0.8, top_p=0.8):
        self.api_url = CLOVAX_URL
        self.api_key = CLOVAX_API_KEY
        self.model = model_name
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.top_p = top_p

        self.input_cost_per_token = 0.00125
        self.output_cose_per_token = 0.005
        self.logger = get_logger("stdout")

    # 정렬 함수
    def natural_key(self, string):
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', string)]

    # 프롬프트 템플릿 로딩 함수
    def load_prompt_template(self, filename):
        with open(os.path.join(Path(__file__).resolve().parent.parent.parent / "templates", filename), "r", encoding="utf-8") as f:
            return f.read()

    def remove_emojis(self, text: str):
        # 이모티콘 유니코드 범위를 정규식으로 정의
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # 이모티콘 (😀-🙏)
            "\U0001F300-\U0001F5FF"  # 기호 & 픽토그램 (🌀-🗿)
            "\U0001F680-\U0001F6FF"  # 교통 & 지도 기호 (🚀-🛳)
            "\U0001F700-\U0001F77F"  # 화학 기호
            "\U0001F780-\U0001F7FF"  # 기하학 모양
            "\U0001F800-\U0001F8FF"  # 보충 화살표
            "\U0001F900-\U0001F9FF"  # 보충 기호 및 픽토그램
            "\U0001FA00-\U0001FA6F"  # 체스, 마작 기호 등
            "\u2600-\u26FF"  # 날씨, 점성술, 장치 기호 (☀-⛿)
            "\u2700-\u27BF"  # 딩뱃 기호 (✀-➿)
            "]+", flags=re.UNICODE
        )
        return emoji_pattern.sub(r'', text)

    # 4.2.1 시스템 프롬프트 생성
    def get_system_prompt(self, large_div_nm: str, middle_div_nm: str):
        # 유형별 시스템 프롬프트 불러오기
        text_prompt_template = self.load_prompt_template("text_prompt.txt")
        table_prompt_template = self.load_prompt_template("table_prompt.txt")
        image_prompt_template = self.load_prompt_template("image_prompt.txt")
        anals_prompt_template = self.load_prompt_template("anals_prompt.txt")
        # ----------------------------
        # 유형별 System Prompt 정의
        # ----------------------------
        text_prompt_template = text_prompt_template.format(
            large_div_nm=large_div_nm,
            middle_div_nm=middle_div_nm
        )

        image_prompt_template = image_prompt_template.format(
            large_div_nm=large_div_nm,
            middle_div_nm=middle_div_nm
        )

        table_prompt_template = table_prompt_template.format(
            large_div_nm=large_div_nm,
            middle_div_nm=middle_div_nm

        )

        anals_prompt_template = anals_prompt_template.format(
            large_div_nm=large_div_nm,
            middle_div_nm=middle_div_nm
        )

        system_prompts = {
            "text": (
                text_prompt_template
            ),
            "image": (
                image_prompt_template
            ),
            "table": (
                table_prompt_template
            ),
            "anals": (
                anals_prompt_template
            )
        }
        return system_prompts

    # 4.2.1 이미지 변환 함수
    def rptc_prompt_analyze_image(self, image_url):
        # GIF 변환 처리
        if image_url.lower().endswith(".gif"):
            try:
                response = requests.get(image_url)
                response.raise_for_status()

                with Image.open(BytesIO(response.content)) as img:
                    if img.mode in ("RGBA", "P"):
                        img = img.convert("RGB")
                    tmp_dir = "/tmp"
                    os.makedirs(tmp_dir, exist_ok=True)
                    filename = f"{uuid.uuid4().hex}.jpeg"
                    output_path = os.path.join(tmp_dir, filename)
                    img.save(output_path, format='JPEG')

                with open(output_path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode("utf-8").replace("\n", "")
                    data_uri = f"data:image/jpeg;base64,{encoded}"
                    return {"type": "dataUri", "data": data_uri}

            except Exception as e:
                return {"type": "url", "data": image_url}

        else:
            # 일반 이미지일 경우 리사이즈 여부 판단
            response = requests.get(image_url)
            img = Image.open(BytesIO(response.content))
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            width, height = img.size
            ratio = max(width / height, height / width)

            # 리사이즈 필요 여부 확인
            if ratio > 5.0 or max(width, height) > 2240 or min(width, height) < 4:
                if width > height:
                    new_width = min(width, 2240)
                    new_height = max(4, int(new_width / (width / height)))
                else:
                    new_height = min(height, 2240)
                    new_width = max(4, int(new_height * (width / height)))

                new_ratio = max(new_width / new_height, new_height / new_width)
                if new_ratio > 5.0:
                    if new_width > new_height:
                        new_width = int(min(new_height * 5, 2240))
                    else:
                        new_height = int(min(new_width * 5, 2240))

                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

                buffer = io.BytesIO()
                img.save(buffer, format="JPEG", quality=85, optimize=True)
                encoded = base64.b64encode(buffer.getvalue()).decode("utf-8").replace("\n", "")
                data_uri = f"data:image/jpeg;base64,{encoded}"
                return {"type": "dataUri", "data": data_uri}
            else:
                return {"type": "url", "data": image_url}

    # ==== 이미지 분석 함수 ====
    def rptc_prompt_image_anals(self, key_name, image_url) -> str:

        if not image_url:
            return "(이미지 URL이 없습니다.)"

        # 이미지 전처리 (url/dataUri 변환)
        img_info = self.rptc_prompt_analyze_image(image_url)

        # SYSTEM (이미지 분석 전용 프롬프트)
        image_anals_prompt_template = self.load_prompt_template("image_analysis_prompt.txt")

        # 이미지 파트
        if img_info["type"] == "url":
            image_part = {"type": "image_url", "imageUrl": {"url": img_info["data"]}}
        else:
            image_part = {"type": "image_url", "dataUri": {"data": img_info["data"]}}

        # 메시지 구성
        image_user_text = "이미지를 분석해 주세요."
        image_messages = [
            {"role": "system", "content": image_anals_prompt_template},
            {"role": "user", "content": [
                {"type": "text", "text": image_user_text},
                image_part
            ]}
        ]

        # 호출
        try:
            img_result = self.run_clovax("",f"{key_name}_IMGANALS", image_messages)
            self.logger.debug(f"[BATCH-TOKEN] IMAGE RUN CLOVA : {json.dumps(img_result, indent=2)}")
            img_text = (img_result or {}).get("response") or ""
            img_text = img_text.strip() if isinstance(img_text, str) else ""
            return img_text if img_text else "(이미지 분석 결과를 가져오지 못했습니다.)"
        except Exception:
            return "(이미지 분석 중 오류가 발생했습니다.)"

    # 텍스트
    def rptc_make_messages_text(self, key_name, merged_value):

        overview = merged_value.get("개요")

        # SYSTEM (텍스트 전용 프롬프트)
        large_div_nm = merged_value.get("LARGE_DIV_NM")
        middle_div_nm = merged_value.get("MIDDLE_DIV_NM")

        text_prompt_template = self.load_prompt_template("text_prompt.txt")
        system_prompt = text_prompt_template.format(
            large_div_nm=large_div_nm
        )

        # USER: 보고서 개요
        report_overview = (
            f"[보고서 개요]\n\n"
            f"    - 탐구명: {overview.get('RSH_NM', None)}\n"
            f"    - 보고서명: {overview.get('RPTC_ID', None)}\n"
            f"    - 학습 목표: {overview.get('LRN_GOAL_CN', None)}\n"
            f"    - 성취 기준: {overview.get('SCCES_STDR_CNS', None)}\n"
            f"    - 학교급: {overview.get('LARGE_DIV_NM', None)}\n"
            f"    - 학년: {overview.get('MIDDLE_DIV_NM', None)}\n"
            f"    - 단원: {overview.get('SMALL_DIV_NM', None)}\n"
        )

        # USER: 이전 문항 맥락 반영 (있으면)
        prior_list = merged_value.get("이전 질문 답변 평가") or []
        prior_text = ""
        if prior_list:  # 빈 리스트가 아니면
            prior_text = f"[이전 질문 답변 평가]\n{prior_list}"

        # USER: 질문/답변
        user_text = (
            f"- 질문(대제목): {merged_value.get('대제목')}\n"
            f"- 질문(소제목): {merged_value.get('소제목')}\n"
            f"- 학생 답변: {merged_value.get('답변')}\n\n"
            f"학생의 답변을 문단 단위로 평가해 주세요. 각 문단은 핵심 내용을 중심으로 짧고 가독성 있게 작성해 주되, 전체 평가는 두 문단 이내로 작성해 주세요.\n"

        )

        return [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "assistant",
                "content": report_overview
            },
            {
                "role": "user",
                "content": prior_text
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text}
                ]
            }
        ]

    # 이미지
    def rptc_make_messages_image(self, key_name, merged_value):

        overview = merged_value.get("개요")

        # SYSTEM (이미지 전용 프롬프트)
        large_div_nm = merged_value.get("LARGE_DIV_NM")
        middle_div_nm = merged_value.get("MIDDLE_DIV_NM")

        image_prompt_template = self.load_prompt_template("image_prompt.txt")
        system_prompt = image_prompt_template.format(
            large_div_nm=large_div_nm
        )

        # USER: 보고서 개요
        report_overview = (
            f"[보고서 개요]\n\n"
            f"    - 탐구명: {overview.get('RSH_NM', None)}\n"
            f"    - 보고서명: {overview.get('RPTC_ID', None)}\n"
            f"    - 학습 목표: {overview.get('LRN_GOAL_CN', None)}\n"
            f"    - 성취 기준: {overview.get('SCCES_STDR_CNS', None)}\n"
            f"    - 학교급: {overview.get('LARGE_DIV_NM', None)}\n"
            f"    - 학년: {overview.get('MIDDLE_DIV_NM', None)}\n"
            f"    - 단원: {overview.get('SMALL_DIV_NM', None)}\n"
        )

        prior_list = merged_value.get("이전 질문 답변 평가") or []
        prior_text = ""
        if prior_list:  # 빈 리스트가 아니면
            prior_text = f"[이전 질문 답변 평가]\n{prior_list}"

        # USER: 질문/답변
        user_text = (
            f"- 질문(대제목): {merged_value.get('대제목')}\n"
            f"- 질문(소제목): {merged_value.get('소제목')}\n"
        )

        image_url = merged_value.get("이미지")

        if image_url:
            # 이미지 자체는 보내지 않고, 분석 텍스트만 포함
            img_analysis_text = self.rptc_prompt_image_anals(key_name, image_url)
            user_text += (f"- 학생 답변(학생이 제출한 이미지를 AI가 분석한 결과): \n{img_analysis_text}\n\n"
                          f"학생의 답변을 문단 단위로 평가해 주세요. 각 문단은 핵심 내용을 중심으로 짧고 가독성 있게 작성해 주되, 전체 평가는 두 문단 이내로 작성해 주세요.\n")

        return [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "assistant",
                "content": report_overview
            },
            {
                "role": "user",
                "content": prior_text
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text}
                ]
            }
        ]

    # 표
    def rptc_make_messages_table(self, key_name, merged_value):

        overview = merged_value.get("개요")

        # SYSTEM (텍스트 전용 프롬프트)
        large_div_nm = merged_value.get("LARGE_DIV_NM")
        middle_div_nm = merged_value.get("MIDDLE_DIV_NM")

        table_prompt_template = self.load_prompt_template("table_prompt.txt")
        system_prompt = table_prompt_template.format(
            large_div_nm=large_div_nm
        )

        # USER: 보고서 개요
        report_overview = (
            f"[보고서 개요]\n\n"
            f"    - 탐구명: {overview.get('RSH_NM', None)}\n"
            f"    - 보고서명: {overview.get('RPTC_ID', None)}\n"
            f"    - 학습 목표: {overview.get('LRN_GOAL_CN', None)}\n"
            f"    - 성취 기준: {overview.get('SCCES_STDR_CNS', None)}\n"
            f"    - 학교급: {overview.get('LARGE_DIV_NM', None)}\n"
            f"    - 학년: {overview.get('MIDDLE_DIV_NM', None)}\n"
            f"    - 단원: {overview.get('SMALL_DIV_NM', None)}\n"
        )

        # USER: 이전 문항 맥락 반영 (있으면)
        prior_list = merged_value.get("이전 질문 답변 평가") or []
        prior_text = ""
        if prior_list:  # 빈 리스트가 아니면
            prior_text = f"[이전 질문 답변 평가]\n{prior_list}"

        # USER: 질문/답변
        user_text = (
            f"- 질문(대제목): {merged_value.get('대제목')}\n"
            f"- 질문(소제목): {merged_value.get('소제목')}\n"
            f"- 학생 답변(표): {merged_value.get('표')}\n\n"
            f"학생의 답변을 문단 단위로 평가해 주세요. 각 문단은 핵심 내용을 중심으로 짧고 가독성 있게 작성해 주되, 전체 평가는 두 문단 이내로 작성해 주세요.\n"
        )

        return [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "assistant",
                "content": report_overview
            },
            {
                "role": "user",
                "content": prior_text
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text}
                ]
            }
        ]

    def rptc_make_messages_anals(self, key_name, merged_value):

        overview = merged_value.get("개요")

        # SYSTEM (분석데이터 전용 프롬프트)
        large_div_nm = merged_value.get("LARGE_DIV_NM")
        middle_div_nm = merged_value.get("MIDDLE_DIV_NM")

        anals_prompt_template = self.load_prompt_template("anals_prompt.txt")
        system_prompt = anals_prompt_template.format(
            large_div_nm=large_div_nm
        )

        # USER: 보고서 개요
        report_overview = (
            f"[보고서 개요]\n\n"
            f"    - 탐구명: {overview.get('RSH_NM', None)}\n"
            f"    - 보고서명: {overview.get('RPTC_ID', None)}\n"
            f"    - 학습 목표: {overview.get('LRN_GOAL_CN', None)}\n"
            f"    - 성취 기준: {overview.get('SCCES_STDR_CNS', None)}\n"
            f"    - 학교급: {overview.get('LARGE_DIV_NM', None)}\n"
            f"    - 학년: {overview.get('MIDDLE_DIV_NM', None)}\n"
            f"    - 단원: {overview.get('SMALL_DIV_NM', None)}\n"
        )

        # USER: 질문/답변
        header_text = (
            "아래는 학생이 제출한 분석데이터입니다. 각 세트의 특이사항을 종합해 평가해 주세요.\n"
            f"- 질문(대제목): {merged_value.get('대제목')}\n"
            f"- 질문(소제목): {merged_value.get('소제목')}\n"
            "- 학생 답변(분석데이터): "
        )

        # USER: 이전 문항 맥락 반영 (있으면)
        prior_list = merged_value.get("이전 질문 답변 평가") or []
        prior_text = ""
        if prior_list:  # 빈 리스트가 아니면
            prior_text = f"[이전 질문 답변 평가]\n{prior_list}"

        # 분석데이터 정리
        anals = merged_value.get("분석데이터") or {}
        lines = []

        # 정렬: 분석1, 분석2, ...
        for set_name in sorted(anals.keys(), key=self.natural_key):
            item = anals.get(set_name) or {}
            types = item.get("세트유형", []) or []

            parts = []  # 해당 분석 세트에 대해 합칠 텍스트 파트

            # 텍스트/표
            for key in ["text", "table"]:
                if item.get(key):
                    parts.append(str(item.get(key)).strip())

            # 이미지 → image_anals로 분석 → 결과 텍스트만 추가
            if "image" in types and item.get("image"):
                img_text = self.rptc_prompt_image_anals(key_name, item.get("image"))
                parts.append(img_text)

            # 분석 세트 한 덩어리 합치기
            if parts:
                lines.append(f"{set_name}: " + " ".join(p for p in parts if p))

        # 최종 USER 텍스트
        user_text = header_text
        if lines:
            user_text += "\n" + "\n".join(lines)
        user_text += "\n\n학생의 답변을 문단 단위로 평가해 주세요. 각 문단은 핵심 내용을 중심으로 짧고 가독성 있게 작성해 주되, 전체 평가는 두 문단 이내로 작성해 주세요.\n"

        return [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "assistant",
                "content": report_overview
            },
            {
                "role": "user",
                "content": prior_text
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text}
                ]
            }
        ]

    # 종합평가
    def make_messages_feedback(self, rptc_id, meta_data: dict, feedback_results: list):

        # SYSTEM (종합 피드백 전용 프롬프트)
        feedback_prompt = self.load_prompt_template("feedback_prompt.txt")
        system_prompt = feedback_prompt

        # USER: 보고서 개요
        report_overview = (
            f"[보고서 개요]\n\n"
            f"    - 탐구명: {meta_data.get('RSH_NM', None)}\n"
            f"    - 보고서명: {meta_data.get('RPTC_ID', None)}\n"
            f"    - 학습 목표: {meta_data.get('LRN_GOAL_CN', None)}\n"
            f"    - 성취 기준: {meta_data.get('SCCES_STDR_CNS', None)}\n"
            f"    - 학교급: {meta_data.get('LARGE_DIV_NM', None)}\n"
            f"    - 학년: {meta_data.get('MIDDLE_DIV_NM', None)}\n"
            f"    - 단원: {meta_data.get('SMALL_DIV_NM', None)}\n"
        )

        # USER: 문항별 요약(모델 응답 요지) 전달
        header = (
            "아래는 문항별 개별 평가 결과입니다. 전체 흐름을 요약하여 종합 피드백을 작성해 주세요.\n"
            "- 각 문항의 핵심 평가 포인트만 압축해 요약\n"
            "- 공통 강점/개선점, 다음 학습 제안 간단히 정리\n"
            "- 불필요한 격려 문구 없이 구체적으로\n"
        )

        # 문항별 블록 정리
        blocks = []
        for r in sorted(feedback_results, key=lambda x: self.natural_key(x.get("q_key", ""))):
            if not r.get("response"):  # 오류 등으로 응답 없으면 스킵
                continue
            block = (
                f"[{r.get('q_key')}] \n"
                f"{r.get('대제목')} - {r.get('소제목')}\n"
                f"{r.get('response')}\n"
            )
            blocks.append(block)

        # USER: 질문별 평가 결과
        user_text = header + "\n".join(blocks)

        return [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "assistant",
                "content": report_overview
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text}
                ]
            }
        ]

    def run_clovax_tokenizer(self, messages):
        url = f"{CLOVAX_TOKEN_URL}/{self.model}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-NCP-CLOVASTUDIO-REQUEST-ID": str(uuid.uuid4()),
            "Content-Type": "application/json"
        }

        payload = {
            "messages": messages,
        }
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            result = response.json()
            def sum_counts(obj) -> int:
                """임의의 중첩 dict/list 구조에서 'count' 키의 숫자 합계를 구한다."""
                total = 0
                if isinstance(obj, dict):
                    val = obj.get("count")
                    if isinstance(val, (int, float)) or (isinstance(val, str) and val.isdigit()):
                        total += int(val)
                    for v in obj.values():
                        total += sum_counts(v)
                elif isinstance(obj, list):
                    for item in obj:
                        total += sum_counts(item)
                return total
            input_token = sum_counts(result)
            self.logger.debug(f"[BATCH-TOKEN] input_token: {input_token}, output_token: {self.max_tokens}, total_tokens: {input_token+self.max_tokens}")

    def run_clovax(self,rptc_id, key_name, messages) -> dict:

        url = f"{self.api_url}/{self.model}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-NCP-CLOVASTUDIO-REQUEST-ID": str(uuid.uuid4()),
            "Content-Type": "application/json"
        }

        payload = {
            "messages": messages,
            "maxTokens": self.max_tokens,
            "temperature": self.temperature,
            "topP": self.top_p,
        }

        max_retries_qpm = 5
        base_wait_qpm = 2
        retries_qpm = 0
        retried_tpm = 0
        # -----------------------------------------------------------
        self.run_clovax_tokenizer(messages=messages)
        # -----------------------------------------------------------
        # 반복 호출
        while True:
            try:
                # ------- QPM (거의 걸릴 일이 없음)--------
                if retries_qpm > 0:
                    wait_time = base_wait_qpm * (2 ** (retries_qpm - 1))
                    print(f"[{key_name}] 재시도 {retries_qpm}/{max_retries_qpm} (대기 {wait_time}s)")
                    time.sleep(wait_time)

                start_time = time.time()

                response = requests.post(url, headers=headers, json=payload)

                end_time = time.time()
                elapsed = round(end_time - start_time, 4)

                # -----------------------------------------------------------
                # 상태 코드 체크
                if response.status_code == 429:
                    # 상세 코드 구분
                    try:
                        error_data = response.json()
                        error_code = error_data.get("status", {}).get("code", "")
                    except Exception:
                        error_code = ""

                    # -----------------------------------------------------------
                    # QPM 제한
                    if error_code == "42900":

                        if retries_qpm >= max_retries_qpm:
                            raise Exception(f"QPM 제한으로 {max_retries_qpm}회 재시도 실패")
                        retries_qpm += 1
                        print(f"[{key_name}] 42900 QPM 제한 - Exponential Backoff 재시도")
                        continue

                    # -----------------------------------------------------------
                    # TPM 제한
                    elif error_code == "42901":
                        if retried_tpm > 3:
                            raise Exception("TPM 제한으로 3회 재시도 후에도 실패")
                        retried_tpm += 1

                        reset_time_str = response.headers.get("x-ratelimit-reset-tokens")
                        if reset_time_str:
                            try:
                                reset_time = float(reset_time_str.replace('s',''))
                                current_time = time.time()
                                #wait_time = max(reset_time - current_time, 1)
                                wait_time = reset_time
                            except ValueError:
                                self.logger.debug(f"[BATCH-ERROR] X-RATELIMIT-RESET-TOKENS 파싱 에러")
                                wait_time = 60  # 파싱 실패 시 안전 대기

                        self.logger.debug(f"[{key_name}] 42901 TPM 제한 - {round(wait_time)}초 대기 후 {retried_tpm}회 재시도")
                        time.sleep(wait_time)
                        continue

                    else:
                        raise Exception(f"429 오류 - 알 수 없는 제한 코드 {error_code}")

                if response.status_code != 200:
                    raise Exception(f"API 요청 실패: {response.status_code}, {response.text}")

                # -----------------------------------------------------------
                # 응답 결과
                result = response.json()

                raw_output = result["result"]["message"]["content"]

                # -----------------------------------------------------------
                # 코드 블록 마크다운 제거
                cleaned_output = re.sub(r"^```(?:json)?|```$", "", raw_output.strip())
                # "독자", "독자가", "독자들이" 제거
                cleaned_output = re.sub(r"\b독자(들이|가)?\b", "", cleaned_output)
                # $ 이스케이프
                if "$" in cleaned_output:
                    cleaned_output = re.sub(r"\$", r"\\$", cleaned_output)

                # -----------------------------------------------------------
                # 토큰 사용량
                usage_output = result["result"]["usage"]
                prompt_tokens = usage_output["promptTokens"]
                completion_tokens = usage_output["completionTokens"]
                total_tokens = usage_output["totalTokens"]

                input_cost = round(prompt_tokens * self.input_cost_per_token, 10)
                output_cost = round(completion_tokens * self.output_cose_per_token, 10)
                total_cost = input_cost + output_cost


                # 응답 헤더를 dict로 변환
                response_headers = dict(response.headers)

                # -----------------------------------------------------------
                # 응답 헤더 추출
                rate_limit_info = {
                    "x-request-id": response_headers.get("x-request-id"),
                    "x-ratelimit-limit-requests": int(response_headers.get("x-ratelimit-limit-requests", "0")),
                    "x-ratelimit-remaining-requests": int(response_headers.get("x-ratelimit-remaining-requests", "0")),
                    "x-ratelimit-reset-requests": response_headers.get("x-ratelimit-reset-requests"),
                    "x-ratelimit-limit-tokens": int(response_headers.get("x-ratelimit-limit-tokens", "0")),
                    "x-ratelimit-remaining-tokens": int(response_headers.get("x-ratelimit-remaining-tokens", "0")),
                    "x-ratelimit-reset-tokens": response_headers.get("x-ratelimit-reset-tokens")
                }

                return {
                    "response": cleaned_output,
                    "input_tokens": prompt_tokens,
                    "output_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                    "total_cost_krw": round(total_cost, 5),
                    "total_time_seconds": elapsed,
                    "response_header": rate_limit_info
                }

            except Exception as e:
                if isinstance(e, Exception) and "QPM 제한" in str(e):
                    raise Exception(f"[ClovaX 호출 실패] {e}")

                if isinstance(e, Exception) and "TPM 제한" in str(e):
                    raise Exception(f"[ClovaX 호출 실패] {e}")

                retries_qpm += 1
                if retries_qpm >= max_retries_qpm:
                    raise Exception(f"[ClovaX 호출 실패] {e}")

                wait_time = base_wait_qpm * (2 ** (retries_qpm - 1))
                print(f"[{key_name}] 예외 발생: {e} - {wait_time}s 대기 후 재시도")
                time.sleep(wait_time)

    # 4. AI 튜터 시작
    def run_ai_tutor(self, rptc_id, parsed_json):
        user_data = parsed_json.get("user_data", {})
        research_data = parsed_json.get("research_data", {}) or {}

        # report_content를 뺀 report_data 한 줄 처리
        report_data = parsed_json.get("report_data", {}) or {}
        report_content  = report_data.get("report_content", {}) or {}

        makers = {
            "text": self.rptc_make_messages_text,
            "image": self.rptc_make_messages_image,
            "table": self.rptc_make_messages_table,
            "anals": self.rptc_make_messages_anals,
        }

        # 모든 질문 키를 미리 None으로 깔아두기 (예: Q1-1, Q1-2, ...)
        response_map = {k: None for k in sorted(report_content.keys(), key=self.natural_key)}

        # 비용/토큰/시간 누적용
        total_input_tokens = 0
        total_output_tokens = 0
        total_tokens = 0
        total_cost_krw = 0.0
        total_time_seconds = 0.0

        # 피드백 메시지 생성을 위한 질문별 요약 리스트
        feedback_results = []

        prior_ctx = []
        MAX_PRIOR_ITEMS = 2

        for q_key in sorted(report_content.keys(), key=self.natural_key):
            self.logger.debug(f"[BATCH-TUTOR] - 보고서 아이디 : {rptc_id}, Q_KEY : {q_key}")
            q_value = report_content[q_key]

            # 레이아웃 필터
            if str(q_value.get("레이아웃", "")).upper() != "Y":
                continue

            merged_value = {
                "개요": research_data,
                "레이아웃": q_value.get("레이아웃"),
                "유형": q_value.get("유형"),
                "대제목": q_value.get("대제목"),
                "소제목": q_value.get("소제목"),
                "답변": q_value.get("답변"),
                "이미지": q_value.get("이미지"),
                "표": q_value.get("표"),
                "분석데이터": q_value.get("분석세트"),
            }

            typ = str(q_value.get("유형", "text")).lower()
            maker = makers.get(typ)
            if not maker:
                #self.logger.info(f"[BATCH-SKIP] 지원하지 않는 유형: {typ} ({q_key})")
                continue

            if prior_ctx:
                merged_value["이전 질문 답변 평가"] = prior_ctx

            # key_name 생성
            key_name = f"{rptc_id}_{q_key}"

            # 메세지 생성
            messages = maker(key_name, merged_value)

            # 질문별 평가 호출
            question_result = self.run_clovax(rptc_id, key_name, messages)
            self.logger.debug(f"[BATCH-TUTOR] {key_name} - RUN CLOVA : {json.dumps(question_result, indent=2)}")
            # 결과 반영
            response_text = question_result.get("response", "")
            response_map[q_key] = response_text if response_text else None

            # 현재 질문/답변/평가 결과 prior_ctx에 누적
            ans_field = None

            if typ == "text":
                ans_field = (q_value.get("답변") or "").strip() or None

            elif typ == "table":
                table_raw = q_value.get("표")
                if table_raw is not None:
                    ans_field = (str(table_raw)).strip() or None

            elif typ == "image":
                ans_field = "(생략)"

            elif type == "anals":
                ans_field = "(생략)"

            prior_ctx.append({
                "질문": q_value.get("소제목") or q_value.get("대제목"),
                "답변": ans_field,
                "평가결과": response_text
            })

            if len(prior_ctx) > MAX_PRIOR_ITEMS:
                del prior_ctx[0:len(prior_ctx) - MAX_PRIOR_ITEMS]

            # 누적 집계
            total_input_tokens += question_result.get("input_tokens", 0)
            total_output_tokens += question_result.get("output_tokens", 0)
            total_tokens += question_result.get("total_tokens", 0)
            total_cost_krw += question_result.get("total_cost_krw", 0.0)
            total_time_seconds += question_result.get("total_time_seconds", 0.0)

            # 종합 피드백용 개별 평가 내용 적재
            feedback_results.append({
                "q_key": q_key,
                "대제목": q_value.get("대제목"),
                "소제목": q_value.get("소제목"),
                "유형": typ,
                "response": response_text
            })

        # 종합 피드백 호출
        feedback_messages = self.make_messages_feedback(rptc_id, research_data, feedback_results)
        feedback_result = None
        if feedback_messages:
            self.logger.debug(f"[BATCH-TUTOR] - 보고서 아이디 : {rptc_id}, Feedback 호출")
            feedback_key = f"{rptc_id}_FEEDBACK"
            feedback_result = self.run_clovax(rptc_id, feedback_key, feedback_messages)

            self.logger.debug(f"[BATCH-TUTOR] {rptc_id} - RUN CLOVA : {json.dumps(feedback_result, indent=2)}")
            total_input_tokens += feedback_result.get("input_tokens", 0)
            total_output_tokens += feedback_result.get("output_tokens", 0)
            total_tokens += feedback_result.get("total_tokens", 0)
            total_cost_krw += feedback_result.get("total_cost_krw", 0.0)
            total_time_seconds += feedback_result.get("total_time_seconds", 0.0)

            # 최종 response 블록에 feedback 텍스트 포함
            response_map["feedback"] = feedback_result.get("response", "")

        # 최종 결과 패키징
        final_result = {
            "rptc_id": rptc_id,
            "rgtr_id": report_data.get("rgtr_id"),
            "stdnt_id": report_data.get("stdnt_id"),
            "response": response_map,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
            "total_cost_krw": round(total_cost_krw, 8),
            "total_time_seconds": round(total_time_seconds, 4),
            "created_at": datetime.now().isoformat(),
        }

        return final_result