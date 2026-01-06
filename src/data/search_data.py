import json
from client import AESClient, ESQuery
from utils.config import RESULT_INDEX
from utils.status_code import response_send
from utils.logger import get_logger

class SearchData:
    def __init__(self):
        self.client = AESClient()
        self.logger = get_logger()

    async def close_connection(self):
        await self.client._close_connection()

    async def get_report_tutor_eval(self, body):

        user_id = body.user_id
        rptc_id = body.rptc_id

        query = ESQuery()
        query.add_filter(logic="must", field="rptc_id", value=rptc_id)
        rptc_data = await self.client.search(index=RESULT_INDEX, query=query, _all=True)

        if not rptc_data:
            return response_send("002")

        _source = rptc_data[0]['_source']
        rptc_result = {k: v for k, v in _source["response"].items() if v is not None}
        res = {}
        res.__setitem__("rptc_id", _source["rptc_id"])
        res.__setitem__("rptc_result", rptc_result)
        res.__setitem__("created_at", _source["created_at"])

        return response_send(code="001", data=res)



