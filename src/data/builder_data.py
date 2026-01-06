import pandas as pd
from client import AESClient, ESQuery
from utils.logger import get_logger
from utils.config import RESULT_INDEX

BASIC_QUESTIONS = [
    "탐구 방법: 탐구 방법에 대해 작성해 주세요.",
    "탐구 내용: 탐구 내용에 대해 작성해 주세요.",
    "탐구 결과: 탐구 결과에 대해 작성해 주세요.",
    "탐구 결론 및 정리"
]
MAX_Q = 10

# 데이터 빌더 - 조회/병합
class DataBuilder:
    def __init__(self, rptc_ids=None):
        self.client = AESClient()
        self.index = {
            "rptc_info": "science-tutor-rptc-info",
            "rptc_layout" : "science-tutor-rptc-detail",
            "anals_info" : "science-tutor-anals-detail",
            "user_info" : "science-tutor-user-info",
            "rptc_since" : RESULT_INDEX
        }
        self.logger = get_logger("stdout")
        self.rptc_ids = rptc_ids

    # 1. 데이터 가져오기
    async def get_rptc_since(self):
        query = ESQuery()
        query.set_sort(field="mdfcn_dt", order="desc")
        query.set_size(1)
        rptc_since = await self.client.search(index=self.index["rptc_since"], query=query, _all=True)
        if rptc_since:
            return rptc_since[0]['_source']['mdfcn_dt']
        else:
            return False

    async def get_rptc_info(self):
        since = await self.get_rptc_since()
        query = ESQuery()
        if since and not self.rptc_ids:
            query.add_range_filter(logic="must", field="MDFCN_DT", g_type="gt", g_value=since)

        if self.rptc_ids:
            query.add_terms_filter(logic="must", field="RPTC_ID.keyword", values=self.rptc_ids)

        rptc_info_dict = [data async for data in self.client.scan(index=self.index["rptc_info"], query=query)]
        rptc_info_df = pd.DataFrame(rptc_info_dict)
        return rptc_info_df

    async def get_rptc_layout(self):
        query = ESQuery()
        # query.add_range_filter > since 조건 추가
        rptc_layout_dict = [data async for data in self.client.scan(index=self.index["rptc_layout"], query=query)]
        rptc_layout_df = pd.DataFrame(rptc_layout_dict)
        return rptc_layout_df

    async def get_rptc_analysis(self):
        query = ESQuery()
        # query.add_range_filter > since 조건 추가
        analysis_dict = [data async for data in self.client.scan(index=self.index["anals_info"], query=query)]
        analysis_df = pd.DataFrame(analysis_dict)
        return analysis_df

    async def get_user_info(self):
        query = ESQuery()
        # query.add_range_filter > since 조건 추가
        user_dict = [data async for data in self.client.scan(index=self.index["user_info"], query=query)]
        login_df = pd.DataFrame(user_dict)
        return login_df

    # 2-1. 보고서 질문 구조 생성
    def build_question_structure(self, row):
        return {
            "is_two_level": pd.notna(row['LAO_CD_2']),
            "sub1_q": row['LAO_CN_1'],
            "sub1_a": row['LAO_SUBMIT_CN_1'],
            "img1_id": row.get('IMG_FILE_ID_1'),
            "img1_path": row.get('IMG_FILE_FLPTH_1'),
            "img1_name": row.get('IMG_FILE_NAME_1'),
            "anals1": row.get('ANALS_DATA_ID_1'),
            "clct1": row.get('CLCT_DATA_ID_1'),
            "data_text1": row.get('DATA_TEXT_1'),
            "title": row['LAO_CN_2_TITLE'] if pd.notna(row['LAO_CN_2_TITLE']) else None,
            "sub2_q": row['LAO_CN_2'] if pd.notna(row['LAO_CN_2']) else None,
            "sub2_a": row['LAO_SUBMIT_CN_2'] if pd.notna(row['LAO_SUBMIT_CN_2']) else None,
            "img2_id": row.get('IMG_FILE_ID_2'),
            "img2_path": row.get('IMG_FILE_FLPTH_2'),
            "img2_name": row.get('IMG_FILE_NAME_2'),
            "anals2": row.get('ANALS_DATA_ID_2'),
            "clct2": row.get('CLCT_DATA_ID_2'),
            "data_text2": row.get('DATA_TEXT_2')
        }

    # 2-2 분석 그릅화
    def build_analysis_grouped(self, analysis_df):
        analysis_grouped = {}
        for anals_id, group in analysis_df.groupby('CLCT_ANALS_DATA_ID'):
            items = []
            for _, row in group.iterrows():
                items.append({
                    "SORT_ORDR": int(row['SORT_ORDR']),
                    "ANALS_CN": row.get('ANALS_CN'),
                    "ANALS_OBJECT": row.get('ANALS_OBJECT'),
                    "ANALS_FILE_ID": row.get('ANALS_FILE_ID')
                })
            analysis_grouped[anals_id] = items

        return analysis_grouped

    # 2-3 보고서별 데이터 초기화
    def build_rptc_init(self, qa_dict, rptc_info_df, rptc_layout_df, analysis_grouped):
        expanded_rows = []

        for _, info_row in rptc_info_df.iterrows():
            rptc_id = info_row['RPTC_ID']
            rptc_type = info_row['RPTC_SE_NM']
            qa_list = qa_dict.get(rptc_id, [])[:MAX_Q]

            record = {"RPTC_ID": rptc_id}

            # 초기화
            for i in range(1, MAX_Q + 1):
                record[f"Q{i}"] = None
                record[f"Q{i}_1"] = None
                record[f"Q{i}_2"] = None
                record[f"A{i}_1"] = None
                record[f"A{i}_2"] = None
                for s in [1, 2]:
                    record[f"IMG_FILE_ID_{i}_{s}"] = None
                    record[f"IMG_FILE_FLPTH_{i}_{s}"] = None
                    record[f"IMG_FILE_NAME_{i}_{s}"] = None
                    record[f"ANALS_DATA_ID_{i}_{s}"] = None
                    record[f"CLCT_DATA_ID_{i}_{s}"] = None
                    record[f"DATA_TEXT_{i}_{s}"] = None
                    record[f"ANALS_CN_LIST_{i}_{s}"] = None
                    record[f"ANALS_OBJECT_LIST_{i}_{s}"] = None
                    record[f"ANALS_FILE_LIST_{i}_{s}"] = None

            # 교사양식보고서 처리
            if rptc_type == "교사양식보고서":
                for i, qa in enumerate(qa_list):
                    q_num = i + 1
                    if qa['is_two_level']:
                        record[f"Q{q_num}"] = qa['title']
                        record[f"Q{q_num}_1"] = qa['sub1_q']
                        record[f"Q{q_num}_2"] = qa['sub2_q']
                        record[f"A{q_num}_1"] = qa['sub1_a']
                        record[f"A{q_num}_2"] = qa['sub2_a']
                    else:
                        record[f"Q{q_num}_1"] = qa['sub1_q']
                        record[f"A{q_num}_1"] = qa['sub1_a']

                    # 이미지/데이터 세팅
                    record[f"IMG_FILE_ID_{q_num}_1"] = qa['img1_id']
                    record[f"IMG_FILE_FLPTH_{q_num}_1"] = qa['img1_path']
                    record[f"IMG_FILE_NAME_{q_num}_1"] = qa['img1_name']
                    record[f"ANALS_DATA_ID_{q_num}_1"] = qa['anals1']
                    record[f"CLCT_DATA_ID_{q_num}_1"] = qa['clct1']
                    record[f"DATA_TEXT_{q_num}_1"] = qa['data_text1']

                    record[f"IMG_FILE_ID_{q_num}_2"] = qa['img2_id']
                    record[f"IMG_FILE_FLPTH_{q_num}_2"] = qa['img2_path']
                    record[f"IMG_FILE_NAME_{q_num}_2"] = qa['img2_name']
                    record[f"ANALS_DATA_ID_{q_num}_2"] = qa['anals2']
                    record[f"CLCT_DATA_ID_{q_num}_2"] = qa['clct2']
                    record[f"DATA_TEXT_{q_num}_2"] = qa['data_text2']

                    # 분석데이터 매핑
                    for s in [1, 2]:
                        anals_id = record[f"ANALS_DATA_ID_{q_num}_{s}"]
                        if pd.notna(anals_id) and anals_id in analysis_grouped:
                            slot_dict = {}
                            for item in analysis_grouped[anals_id]:
                                so = item['SORT_ORDR']
                                if so not in slot_dict:
                                    slot_dict[so] = {"CN": "없음", "FILE": "없음", "OBJ": "없음"}
                                if pd.notna(item['ANALS_CN']):
                                    slot_dict[so]["CN"] = str(item['ANALS_CN'])
                                if pd.notna(item['ANALS_OBJECT']):
                                    slot_dict[so]["OBJ"] = str(item['ANALS_OBJECT'])
                                if pd.notna(item['ANALS_FILE_ID']):
                                    slot_dict[so]["FILE"] = str(item['ANALS_FILE_ID'])

                            sorted_slots = [slot_dict[k] for k in sorted(slot_dict.keys())]

                            cn_list = [slot["CN"] for slot in sorted_slots]
                            file_list = [slot["FILE"] for slot in sorted_slots]
                            obj_list = [slot["OBJ"] for slot in sorted_slots]

                            record[f"ANALS_CN_LIST_{q_num}_{s}"] = "$$$".join(cn_list)  # <- 변경
                            record[f"ANALS_OBJECT_LIST_{q_num}_{s}"] = "$$$".join(obj_list)  # <- 변경
                            record[f"ANALS_FILE_LIST_{q_num}_{s}"] = "$$$".join(file_list)  # <- 변경

            # 기본양식보고서 처리
            elif rptc_type == "기본양식보고서":
                layout_rows = rptc_layout_df[rptc_layout_df['RPTC_ID'] == rptc_id].reset_index(drop=True)
                for i, q_text in enumerate(BASIC_QUESTIONS):
                    q_num = i + 1
                    if i < len(layout_rows):
                        layout_row = layout_rows.iloc[i]
                        is_two_level = (
                            pd.notna(layout_row['LAO_SUBMIT_CN_2']) or
                            pd.notna(layout_row['IMG_FILE_ID_2']) or
                            pd.notna(layout_row['ANALS_DATA_ID_2'])
                        )
                        if is_two_level:
                            record[f"Q{q_num}"] = q_text
                            record[f"A{q_num}_1"] = layout_row['LAO_SUBMIT_CN_1']
                            record[f"A{q_num}_2"] = layout_row['LAO_SUBMIT_CN_2']
                        else:
                            record[f"Q{q_num}_1"] = q_text
                            record[f"A{q_num}_1"] = layout_row['LAO_SUBMIT_CN_1']

                        # 이미지/데이터 세팅
                        record[f"IMG_FILE_ID_{q_num}_1"] = layout_row['IMG_FILE_ID_1']
                        record[f"IMG_FILE_FLPTH_{q_num}_1"] = layout_row['IMG_FILE_FLPTH_1']
                        record[f"IMG_FILE_NAME_{q_num}_1"] = layout_row['IMG_FILE_NAME_1']
                        record[f"ANALS_DATA_ID_{q_num}_1"] = layout_row['ANALS_DATA_ID_1']
                        record[f"CLCT_DATA_ID_{q_num}_1"] = layout_row['CLCT_DATA_ID_1']
                        record[f"DATA_TEXT_{q_num}_1"] = layout_row['DATA_TEXT_1']

                        if is_two_level:
                            record[f"IMG_FILE_ID_{q_num}_2"] = layout_row['IMG_FILE_ID_2']
                            record[f"IMG_FILE_FLPTH_{q_num}_2"] = layout_row['IMG_FILE_FLPTH_2']
                            record[f"IMG_FILE_NAME_{q_num}_2"] = layout_row['IMG_FILE_NAME_2']
                            record[f"ANALS_DATA_ID_{q_num}_2"] = layout_row['ANALS_DATA_ID_2']
                            record[f"CLCT_DATA_ID_{q_num}_2"] = layout_row['CLCT_DATA_ID_2']
                            record[f"DATA_TEXT_{q_num}_2"] = layout_row['DATA_TEXT_2']

                        # 분석데이터 매핑
                        for s in [1, 2]:
                            anals_id = record[f"ANALS_DATA_ID_{q_num}_{s}"]
                            if pd.notna(anals_id) and anals_id in analysis_grouped:
                                items = analysis_grouped[anals_id]
                                record[f"ANALS_CN_LIST_{q_num}_{s}"] = '|'.join(
                                    [str(x['ANALS_CN']) for x in items if pd.notna(x['ANALS_CN'])])
                                record[f"ANALS_OBJECT_LIST_{q_num}_{s}"] = '|'.join(
                                    [str(x['ANALS_OBJECT']) for x in items if pd.notna(x['ANALS_OBJECT'])])
                                record[f"ANALS_FILE_LIST_{q_num}_{s}"] = '|'.join(
                                    [str(x['ANALS_FILE_ID']) for x in items if pd.notna(x['ANALS_FILE_ID'])])
                    else:
                        record[f"Q{q_num}_1"] = q_text

            expanded_rows.append(record)
        return expanded_rows

    # 2-4 컬럼 정렬
    def build_culumn_sorting(self, base_cols: list):
        ordered_cols = base_cols.copy()
        for i in range(1, MAX_Q + 1):
            ordered_cols.append(f"Q{i}")
            ordered_cols.append(f"Q{i}_1")
            ordered_cols.append(f"A{i}_1")
            ordered_cols.extend([
                f"IMG_FILE_ID_{i}_1", f"IMG_FILE_FLPTH_{i}_1", f"IMG_FILE_NAME_{i}_1",
                f"ANALS_DATA_ID_{i}_1", f"ANALS_CN_LIST_{i}_1", f"ANALS_OBJECT_LIST_{i}_1", f"ANALS_FILE_LIST_{i}_1",
                f"CLCT_DATA_ID_{i}_1", f"DATA_TEXT_{i}_1"
            ])
            ordered_cols.append(f"Q{i}_2")
            ordered_cols.append(f"A{i}_2")
            ordered_cols.extend([
                f"IMG_FILE_ID_{i}_2", f"IMG_FILE_FLPTH_{i}_2", f"IMG_FILE_NAME_{i}_2",
                f"ANALS_DATA_ID_{i}_2", f"ANALS_CN_LIST_{i}_2", f"ANALS_OBJECT_LIST_{i}_2", f"ANALS_FILE_LIST_{i}_2",
                f"CLCT_DATA_ID_{i}_2", f"DATA_TEXT_{i}_2"
            ])

        return ordered_cols