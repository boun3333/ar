STATUS_CODE = {
    '001': '정상 처리되었습니다.',
    '002': '보고서 평가 데이터가 존재하지 않습니다.',
    '003': '사용자 아이디가 존재하지 않습니다.',
    "999": "시스템에서 처리 중 알 수 없는 오류가 발생했습니다. 잠시 후 다시 시도해주시고, 문제가 지속되면 관리자에게 문의 바랍니다."
}

def response_send(code: str, data: dict = None):
    response = {'response': {}}
    response['response']['result'] = {'code': code, 'message': STATUS_CODE[code]}
    if data:
        response['response'].update(data)
    return response