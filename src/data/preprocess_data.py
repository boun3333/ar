import json
import pandas as pd
import requests
from PIL import Image
import io
import base64
from io import BytesIO
import uuid

BASE_URL = "https://science-on.kosac.re.kr/upload/ON/"
FILPH_URL = "https://science-on.kosac.re.kr/upload"
valid_ext = [".jpg", ".jpeg", ".png", ".gif"]


# 유형 판별 함수
def preprocessing_data_type(answer, image, table_val, anals_dict):

    has_anals = any([
        bool(anals_dict.get("분석데이터내용")),
        bool(anals_dict.get("분석데이터이미지")),
        bool(anals_dict.get("분석데이터표")),
    ])
    if answer: return "text"
    if image: return "image"
    if table_val: return "table"
    if has_anals: return "anals"

    return None

# 분석세트 생성 함수
def preprocessing_analysis_sets(cn, img, tbl):

    def _to_list(value):
        if value is None:
            return []
        s = str(value)
        parts = s.split('$$$') if '$$$' in s else [s]
        out = []
        for p in parts:
            p = str(p).strip()
            if p == "" or p.lower() in {"없음", "none", "null"}:
                out.append(None)
            else:
                out.append(p)
        return out

    cn_list  = _to_list(cn)
    img_list = _to_list(img)
    tbl_list = _to_list(tbl)

    max_len = max(len(cn_list), len(img_list), len(tbl_list))
    if max_len == 0:
        return None

    sets = {}
    for i in range(max_len):
        fields = {}
        if i < len(cn_list) and cn_list[i] is not None:
            fields["text"] = cn_list[i]
        if i < len(img_list) and img_list[i] is not None:
            fields["image"] = img_list[i]
        if i < len(tbl_list) and tbl_list[i] is not None:
            fields["table"] = tbl_list[i]

        if fields:
            types = list(fields.keys())

            item = {"세트유형": types, **fields}
            sets[f"분석{i + 1}"] = item

    return sets or None

# 표 변환 함수
def preprocessing_parse_table(value, max_lines=30):
    text_table = None

    # 문자열 속 첫 번째 JSON 객체만 안전하게 파싱
    def _json_first(s):
        if isinstance(s, (dict, list)):  # 이미 파싱된 객체면 그대로 사용
            return s
        if not isinstance(s, str):
            raise ValueError("지원하지 않는 입력 타입입니다.")

        dec = json.JSONDecoder()
        i = 0
        n = len(s)
        # 앞쪽 공백/개행 스킵
        while i < n and s[i].isspace():
            i += 1
        # 가능한 모든 위치에서 첫 JSON 시도 (잡음/로그 텍스트가 앞에 붙은 경우 대비)
        while i < n:
            try:
                obj, end = dec.raw_decode(s, i)
                # 뒤에 또 뭐가 있더라도(로그/두 번째 JSON 등) 첫 객체만 사용
                return obj
            except json.JSONDecodeError:
                # 다음 '{'를 찾아서 거기서 다시 시도
                j = s.find('{', i + 1)
                if j == -1:
                    # 배열이 최상위 루트일 수도 있으니 '['도 탐색
                    j = s.find('[', i + 1)
                if j == -1:
                    break
                i = j
        # 최후 시도: 양 끝의 첫 '{'와 마지막 '}' 사이만 잘라 시도
        l = s.find('{')
        r = s.rfind('}')
        if l != -1 and r != -1 and r > l:
            return json.loads(s[l:r+1])
        raise json.JSONDecodeError("첫 JSON 객체를 찾을 수 없습니다.", s, 0)

    try:
        jd = _json_first(value)

        sheet = None
        full_sheet_meta = None  # 헤더 등 메타 접근용

        # --- Case 1: {"sheets": {...}} (Luckysheet 계열)
        if isinstance(jd, dict) and "sheets" in jd:
            sheets = jd.get("sheets", {})
            active_idx = jd.get("activeSheetIndex", None)

            # 후보 수집: (이름, dataTable(dict), 원시시트)
            candidates = []
            for name, s in sheets.items():
                dt = s.get("data", {}).get("dataTable", {})
                candidates.append((name, dt if isinstance(dt, dict) else {}, s))

            # 1) isSelected == True + 비어있지 않은 시트
            chosen = next(((n, dt, s) for n, dt, s in candidates if s.get("isSelected") is True and dt), None)
            # 2) activeSheetIndex + 비어있지 않은 시트
            if not chosen and active_idx is not None:
                chosen = next(((n, dt, s) for n, dt, s in candidates if s.get("index") == active_idx and dt), None)
            # 3) 비어있지 않은 첫 시트 -- 조건변경필요
            if not chosen:
                chosen = next(((n, dt, s) for n, dt, s in candidates if dt), None)
            # 4) 전부 비었으면 종료
            if not chosen:
                return "표 데이터를 찾을 수 없습니다."

            first_sheet_name, sheet, full_sheet_meta = chosen

        # --- Case 2: {"Sheet1": {...}} 형태 (행/열 딕셔너리 그대로)
        else:
            if not jd:
                return "표 데이터를 찾을 수 없습니다."
            first_sheet_name, sheet = next(((n, s) for n, s in jd.items() if isinstance(s, dict) and s), (None, None))
            if sheet is None:
                return "표 데이터를 찾을 수 없습니다."
            # 안전가드: 시트 전체를 잡았을 가능성 → data.dataTable 강제
            if "data" in sheet and isinstance(sheet["data"], dict) and "dataTable" in sheet["data"]:
                full_sheet_meta = sheet
                sheet = sheet["data"]["dataTable"] or {}

        # 시트가 dict인지/비었는지 확인
        if not isinstance(sheet, dict) or not sheet:
            return "표 데이터가 비어 있습니다."

        # 행 키 수집 (숫자 문자열만) – 비면 종료
        row_keys = sorted([str(k) for k in sheet.keys() if str(k).isdigit()], key=lambda x: int(x))
        if not row_keys:
            return "표 데이터가 비어 있습니다."

        # 열 키: 모든 행의 열 키 합집합 (숫자 문자열만) – 비면 종료
        col_keys_set = set()
        for rk in row_keys:
            row = sheet.get(rk, {})
            if isinstance(row, dict):
                for ck in row.keys():
                    if str(ck).isdigit():
                        col_keys_set.add(str(ck))
        col_keys = sorted(list(col_keys_set), key=lambda x: int(x))
        if not col_keys:
            return "표 데이터가 비어 있습니다."

        # 텍스트 테이블 생성 (value / v 둘 다 대응)
        lines = []
        for rk in row_keys:
            row = sheet.get(rk, {})
            if not isinstance(row, dict):
                row = {}
            row_values = []
            for ck in col_keys:
                cell = row.get(ck, {})
                if isinstance(cell, dict):
                    v = cell.get("value", cell.get("v", ""))
                else:
                    v = cell
                row_values.append("" if v is None else str(v))
            lines.append(" | ".join(row_values))

        # (옵션) 헤더가 colHeaderData에 있으면 첫 줄로 추가
        if full_sheet_meta and isinstance(full_sheet_meta, dict):
            chd = full_sheet_meta.get("colHeaderData", {}).get("dataTable", {}).get("0", {})
            if isinstance(chd, dict) and chd:
                header_vals = []
                for ck in col_keys:
                    hcell = chd.get(ck, {})
                    if isinstance(hcell, dict):
                        hv = hcell.get("value", hcell.get("v", ""))
                    else:
                        hv = hcell
                    header_vals.append("" if hv is None else str(hv))
                if any(h for h in header_vals):
                    lines.insert(0, " | ".join(header_vals))

        # 줄 수 제한
        if len(lines) > max_lines:
            lines = lines[:max_lines] + ["...(이후 생략)"]

        return "\n".join(lines)

    except Exception as e:
        # 너무 긴 value 로그 폭주 방지: 앞부분만 잘라서 출력
        s = value if isinstance(value, str) else str(value)
        snippet = s[:800] + ("...(trimmed)" if len(s) > 800 else "")
        print(f"[parse_table] 표 변환 실패: {e} | snippet={snippet}")
        return text_table



# 3.1 데이터 전처리
def preprocessing_rptc(merge_df):
    proprocess_data = {}

    for _, row in merge_df.iterrows():
        rptc_id = row['RPTC_ID']
        scces_code = row['SCCES_STDR_CODE'] if pd.notna(row['SCCES_STDR_CODE']) else row.get('ALT_SCCES_STDR_CODE')
        scces_cns = row['SCCES_STDR_CNS'] if pd.notna(row['SCCES_STDR_CNS']) else row.get('ALT_SCCES_STDR_CNS')

        # report_content 초기화
        report_content = {}

        for i in range(1, 11):
            q_title = row.get(f"Q{i}")  # 대제목

            for s in [1, 2]:
                sub_key = f"Q{i}-{s}"

                q_key = f"Q{i}_{s}"
                a_key = f"A{i}_{s}"
                table_key = f"DATA_TEXT_{i}_{s}"
                anals_cn_key = f"ANALS_CN_LIST_{i}_{s}"
                anals_obj_key = f"ANALS_OBJECT_LIST_{i}_{s}"
                anals_file_key = f"ANALS_FILE_LIST_{i}_{s}"
                img_id_key = f"IMG_FILE_ID_{i}_{s}"
                img_flpth_key = f"IMG_FILE_FLPTH_{i}_{s}"
                img_name_key = f"IMG_FILE_NAME_{i}_{s}"

                question = row.get(q_key)
                answer = row.get(a_key)

                table_raw = str(row.get(table_key)) if pd.notna(row.get(table_key)) else None
                table_text = None
                if table_raw and table_raw.lower() != "nan":
                    parsed = preprocessing_parse_table(table_raw)  # JSON이면 파싱, 아니면 None 반환
                    table_text = parsed if parsed else (str(table_raw).strip() or None)

                anals_cn_val = str(row.get(anals_cn_key)) if pd.notna(row.get(anals_cn_key)) else None

                # 분석 이미지 목록 정규화
                anals_img_raw = str(row.get(anals_file_key)) if pd.notna(row.get(anals_file_key)) else None
                anals_img_val = None
                if anals_img_raw:
                    anals_images = []
                    for img_item in anals_img_raw.split("$$$"):  # <- 변경
                        img_item = img_item.strip()
                        if img_item and img_item.lower().endswith(tuple(valid_ext)):
                            anals_images.append(BASE_URL + img_item)
                        else:
                            anals_images.append(img_item if img_item else None)
                    anals_img_val = "$$$".join(
                        [img for img in anals_images if img]) if anals_images else None  # <- 변경

                # 분석데이터표 텍스트 치환
                anals_obj_raw = row.get(anals_obj_key)
                anals_obj_text = None
                if pd.notna(anals_obj_raw):
                    parts = str(anals_obj_raw).split("$$$")  # <- 변경
                    conv_parts = []
                    for part in parts:
                        raw_piece = part
                        piece = part.strip()

                        if piece == "없음":
                            conv_parts.append(raw_piece)
                            continue
                        if piece == "":
                            conv_parts.append(raw_piece)
                            continue

                        try:
                            txt = preprocessing_parse_table(piece)
                            conv_parts.append(txt if txt is not None else raw_piece)
                        except Exception:
                            conv_parts.append(raw_piece)

                    anals_obj_text = "$$$".join(conv_parts)  # <- 변경
                else:
                    anals_obj_text = None

                # -----------------------------------------------------
                # [분석세트] 생성 (최종 저장은 분석세트만)
                analysis_sets = preprocessing_analysis_sets(
                    anals_cn_val,
                    anals_img_val,
                    anals_obj_text
                )

                # -----------------------------------------------------
                # [일반 이미지] 처리 (문항 이미지)
                img_id = row.get(img_id_key)
                img_flpth = row.get(img_flpth_key)
                img_name = row.get(img_name_key)

                image_val = None
                if pd.notna(img_name):
                    img_name_lower = str(img_name).lower()
                    if any(img_name_lower.endswith(ext) for ext in valid_ext):
                        image_val = FILPH_URL + str(img_flpth) + "/" + str(img_name)
                    else:
                        image_val = BASE_URL + str(img_id) if pd.notna(img_id) else None
                elif pd.notna(img_id):
                    image_val = BASE_URL + str(img_id)

                # -----------------------------------------------------
                # 레이아웃 여부 (대/소제목 존재 여부)
                has_layout = (pd.notna(q_title) and str(q_title).strip()) or (
                            pd.notna(question) and str(question).strip())
                layout_flag = "Y" if has_layout else "N"

                # -----------------------------------------------------
                # 기본 필드 정리
                answer_clean = str(answer).strip() if pd.notna(answer) and str(answer).strip() else None
                image_clean = image_val if image_val else None
                final_table = table_text  # 표는 텍스트 저장

                # -----------------------------------------------------
                # data_type() 판정을 위한 임시 분석딕트(원본키 포함)
                # 저장용은 '분석세트'만 남기되, 유형 판정은 기존 시그니처를 존중
                anals_for_type = {
                    "분석데이터내용": anals_cn_val,
                    "분석데이터이미지": anals_img_val,
                    "분석데이터표": anals_obj_text,
                    "분석세트": analysis_sets
                }

                # -----------------------------------------------------
                # 데이터 유형 판별
                if layout_flag == "Y":
                    q_type = preprocessing_data_type(answer_clean, image_clean, final_table, anals_for_type)
                    if q_type is None:
                        q_type = "무응답"
                        image_clean = None
                        final_table = None
                        analysis_sets = None
                else:
                    q_type = None

                # -----------------------------------------------------
                # 최종 저장 (분석세트만 보존)
                item = {
                    "레이아웃": layout_flag,
                    "유형": q_type,
                    "대제목": str(q_title).strip() if pd.notna(q_title) else None,
                    "소제목": str(question).strip() if pd.notna(question) else None,
                    "답변": answer_clean,
                    "이미지": image_clean,
                    "표": final_table
                }
                if analysis_sets is not None:
                    item["분석세트"] = analysis_sets

                report_content[sub_key] = item

        # ---------------------------------------------------------
        # 한 행(row) 단위 결과 묶음 구성 & 저장
        record_dict = {
            "user_data": {
                "RGTR_ID": row.get("RGTR_ID"),
                "STDNT_ID": row.get("STDNT_ID")
            },
            "research_data": {
                "RSH_NM": row["RSH_NM"],
                "RSH_BGNG_DT": row["RSH_BGNG_DT"],
                "RSH_END_DT": row["RSH_END_DT"],
                "LARGE_DIV_NM": row.get("LARGE_DIV_NM"),
                "MIDDLE_DIV_NM": row.get("MIDDLE_DIV_NM"),
                "SMALL_DIV_NM": row.get("SMALL_DIV_NM"),
                "LRN_GOAL_CN": row["LRN_GOAL_CN"],
                "SCCES_STDR_CODE": scces_code,
                "SCCES_STDR_CNS": scces_cns
            },
            "report_data": {
                "RPTC_ID": row["RPTC_ID"],
                "RPTC_NM": row["RPTC_NM"],
                "RPTC_NMPR_SE_CD": row["RPTC_NMPR_SE_CD"],
                "RPTC_SE_NM": row["RPTC_SE_NM"],
                "MDFCN_DT": row["MDFCN_DT"],
                "report_content": report_content
            }
        }

        proprocess_data[rptc_id] = {
            "json_str": json.dumps(record_dict, ensure_ascii=False),
            "MDFCN_DT": row["MDFCN_DT"]
        }

    sorted_data = dict(
        sorted(
            proprocess_data.items(),
            key=lambda item: item[1]["MDFCN_DT"]  # MDFCN_DT 값 기준
        )
    )

    return sorted_data