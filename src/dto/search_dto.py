from pydantic import BaseModel

class TutorManualReportListDto(BaseModel):
    rptc_list: list = []

class TutorReportSearchDto(BaseModel):
    user_id: str
    rptc_id: str