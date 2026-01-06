from typing import Union


class ESQuery(dict):
    def __init__(self):
        super(ESQuery, self).__init__()

    def set_source(self, source_list: list):
        if self.get('_source') is None:
            self.__setitem__('_source', [])
        for source in source_list:
            self['_source'].append(source)

    def set_sort(self, field: str, order: str):
        assert order in ['asc', 'desc'], 'order must be one of "asc", "desc"'
        if self.get('sort') is None:
            self.__setitem__('sort', [])
        self['sort'].append({
            field: {
                'order': order
            }
        })

    def set_size(self, size: int, start: int = 0):
        """
        검색 사이즈 설정
        :param size: 검색할 document 수
        :param start: 검색 시작점
        :return:
        """

        if self.get('from') is None:
            self.__setitem__('from', 0)
        self['from'] = start

        if self.get('size') is None:
            self.__setitem__('size', 0)
        self['size'] = size

    def agg_setting(self):
        if self.get('aggs') is None:
            # self.__setitem__('size', 0)
            self.__setitem__('aggs', {})

    def query_setting(self):
        if self.get('query') is None:
            self.__setitem__('query', {'bool': {'must': [], 'must_not': [], 'should': []}})

    def query_function_setting(self):
        if self.get('query') is None:
            self.__setitem__('query', {
                'function_score': {'query': {'bool': {'must': [], 'must_not': [], 'should': []}}, 'functions': []}})

    def add_filter(self, logic: str, field: str, value):
        """ logic:
            - must: AND
            - must_not: NAND
            - should: OR
        """
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should"'
        self.query_setting()
        self['query']['bool'][logic].append({'match_phrase': {field: str(value)}})

    def add_match_filter(self, logic: str, field: str, value):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should"'
        self.query_setting()
        self['query']['bool'][logic].append({'match': {field: str(value)}})

    def add_match_all(self, logic):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should"'
        self.query_setting()
        self['query']['bool'][logic].append({'match_all': {}})

    def add_range_filter(
            self, logic: str, field: str,
            g_type: str = None, g_value=None,
            l_type: str = None, l_value=None):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one off "must", "must_not", "should"'
        assert (g_type is not None and g_value is not None) or (l_type is not None and l_value is not None), \
            'one of g and l must be exist'
        assert (g_type in [None, 'gt', 'gte']), 'g_type must be gt or gte or None'
        assert (l_type in [None, 'lt', 'lte']), 'l_type must be lt or lte or None'

        self.query_setting()
        field_dic = {}
        if g_type is not None:
            assert g_value is not None, 'g_value must be exist'
            field_dic[g_type] = g_value
        if l_type is not None:
            assert l_value is not None, 'l_value must be exist'
            field_dic[l_type] = l_value
        self['query']['bool'][logic].append({'range': {field: field_dic}})

    def add_wildcard_filter(self, logic: str, field: str, value: str):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should"'
        self.query_setting()
        self['query']['bool'][logic].append({"wildcard": {f'{field}.keyword': {"value": f'*{str(value)}*'}}})

    def add_exist_filter(self, logic: str, field: str):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one off "must", "must_not", "should"'

        self.query_setting()
        self['query']['bool'][logic].append({'exists': {'field': field}})

    def add_terms_filter(self, logic: str, field: str, values: list):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one off "must", "must_not", "should"'

        self.query_setting()
        self['query']['bool'][logic].append({'terms': {field: values}})

    def add_multi_filter(self, logic: str, fields: list, query: str, score_type: str = "best_fields"):
        """
        특정 필드 값을 리스트 내에 있는 것으로만 제한하는 필터 추가 함수
        :param logic: 로직
            - must: AND
            - must_not: NAND
            - should: OR
        :param fields: 필드명
        :param query: 쿼리
        :param score_type: 점수계산방법
        :return:
        """
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should".'

        self.query_setting()
        self['query']['bool'][logic].append({'multi_match': {"fields": fields, "query": query, "type": score_type}})

    def add_sub_filter(self, logic: str, query: dict):
        """ logictime:
            - must: AND
            - must_not: NAND
            - should: OR
        """
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one off "must", "must_not", "should"'
        self.query_setting()
        self['query']['bool'][logic].append(query)

    def extract_sub_filter(self) -> dict:
        return self['query']

    def add_terms(self, name: str, field: str, size: int = 65535):
        self.agg_setting()
        self['aggs'].__setitem__(name, {'terms': {'field': field, 'size': size}})

    def add_aggtype(self, name: str, field: str, agg_type: str):
        self.agg_setting()
        self['aggs'].__setitem__(name, {agg_type: {'field': field}})

    def add_sub_terms(self, term_names: Union[str, list], sub_name: str, field: str, size: int = 65535):
        if type(term_names) == list:
            eval_code = 'self'
            for name in term_names:
                eval_code += f"['aggs']['{name}']"
            eval_code += ".__setitem__('aggs', {'%s': {'terms': {'field': '%s', 'size': %d}}})" % (sub_name, field,
                                                                                                   size)
            eval(eval_code)
        else:
            if self['aggs'].get(term_names) is None:
                raise ValueError(f'term_name {term_names} is not found')
            self['aggs'][term_names].__setitem__('aggs', {sub_name: {'terms': {'field': field, 'size': size}}})

    def add_sub_aggtype(self, term_names: Union[str, list], sub_name: str, field: str, agg_type: str):
        if type(term_names) == list:
            eval_code = 'self'
            for name in term_names:
                eval_code += f"['aggs']['{name}']"
            eval_code += ".__setitem__('aggs', {'%s': {'%s': {'field': '%s'}}})" % (sub_name, agg_type, field)
            eval(eval_code)
        else:
            if self['aggs'].get(term_names) is None:
                raise ValueError(f'term_name {term_names} is not found')
            self['aggs'][term_names].__setitem__('aggs', {sub_name: {agg_type: {'field': field}}})

    def add_agg_sub_query(self, term_name: str, sub_query: dict):
        """
        sub aggregations 서브 조건 추가 함수
        :param term_name: 상위 terms 이름
        :param sub_query: 서브 조건 쿼리
        :return:
        """
        if self['aggs'].get(term_name) is None:
            raise ValueError(f'term_name {term_name} is not found.')
        self['aggs'][term_name].__setitem__('aggs', sub_query)

    def add_date_histogram(self, name: str, field: str, interval: Union[int, str]):
        assert type(interval) == str or interval > 0
        self.agg_setting()
        self['aggs'].__setitem__(name, {'date_histogram': {'field': field, 'interval': interval}})

    # function_score
    def add_function_filter(self, logic: str, field: str, value):
        """ logic:
            - must: AND
            - must_not: NAND
            - should: OR
        """
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should"'
        self.query_function_setting()
        self['query']['function_score']["query"]["bool"][logic].append({'match_phrase': {field: str(value)}})

    def add_function_term_filter(self, logic: str, field: str, value):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should"'
        self.query_function_setting()
        self['query']['function_score']['query']['bool'][logic].append({'term': {field: str(value)}})

    def add_function_multi_filter(self, logic: str, fields: list, query: str, score_type: str = 'best_fields'):
        assert logic in ['must', 'must_not', 'should'], \
            'logic must be one of "must", "must_not", "should".'

        self.query_function_setting()
        self['query']['function_score']["query"]["bool"][logic].append(
            {'multi_match': {"fields": fields, "query": query, "type": score_type}})

    def add_function_field_value_factor(self, field: str, factor: float, modifier: str = "log1p", missing: float = 1):
        self.query_function_setting()
        self['query']['function_score']["functions"].append(
            {'field_value_factor': {"field": field, "factor": factor, "modifier": modifier, "missing": missing}})

    def add_function_score_mode(self, mode: str = 'sum'):
        """
        mode : ['multiply', 'sum', 'avg', 'max', 'min']
        :param mode: 점수 합산 방법
        :return:
        """
        self.query_function_setting()
        self['query']['function_score'].__setitem__('score_mode', mode)

    def add_function_boost_mode(self, mode: str = 'sum'):
        """
        mode : ['multiply', 'replace', 'sum', 'avg', 'max', 'min']
        :param mode: 점수 합산 방법
        :return:
        """
        self.query_function_setting()
        self['query']['function_score'].__setitem__('boost_mode', mode)

    def add_function_filter_exists(self, field: str, weight: float):
        """
        :param field: 존재유무 필드명
        :param weight: 필드가 존재하는 경우 가중치
        :return:
        """
        self.query_function_setting()
        self['query']['function_score']['functions'].append({'filter': {'exists': {'field': field}}, 'weight': weight})

    def add_function_gauss(self, field: str, origin: str, scale: str, offset: str, decay: float):
        """
        :param field: timestamp 필드
        :param origin: 시간의 기준점
        :param scale: 감소할 시간 가중치
        :param offset: 가중치 영향을 안받는 시간
        :param decay: 가중치가 감소하는 비율
        :return:
        """
        self.query_function_setting()
        self['query']['function_score']['functions'].append(
            {'gauss': {field: {'origin': origin, 'scale': scale, 'offset': offset, 'decay': decay}}})

    def add_function_script_report_score(self, field: str, gte_value: int):
        self.query_function_setting()
        self['query']['function_score']['functions'].append({"filter": {"range": {field: {"gte": gte_value}}},
                                                             "script_score": {
                                                                 "script": {
                                                                     "source": """
                                                                        double normalized_score = Math.min(100, doc['score'].value);
                                                                        return normalized_score;
                                                                     """
                                                                 }
                                                             }})

    def add_function_script_popular_score(self, like_weight: float, view_weight: float, download_weight: float):
        self.query_function_setting()
        self['query']['function_score']['functions'].append({"script_score": {
            "script": {
                "source": """
                    double like_score = (doc.containsKey('like_cnt') && !doc['like_cnt'].empty && params.like_max > 0) ? doc['like_cnt'].value / params.like_max : 0;
                    double view_score = (doc.containsKey('view_cnt') && !doc['view_cnt'].empty && params.view_max > 0) ? doc['view_cnt'].value / params.view_max : 0;
                    double load_score = (doc.containsKey('load_cnt') && !doc['load_cnt'].empty && params.load_max > 0) ? doc['load_cnt'].value / params.load_max : 0;

                    double weighted_score = (
                      like_score * params.like_weight +
                      view_score * params.view_weight +
                      load_score * params.load_weight
                    );
                    return Math.min(100, weighted_score * 100);
                """,
                "params": {
                    "like_weight": 0.4,
                    "view_weight": 0.2,
                    "load_weight": 0.4,
                    "like_max": like_weight,
                    "view_max": view_weight,
                    "load_max": download_weight
                }
            }
        }})

    def add_function_script_bm25_score(self, max_score: float):
        self.query_function_setting()
        self['query']['function_score']['functions'].append({"script_score": {
            "script": {
                "source": """
                    double max_score = params.max_bm25_score;
                    double normalized_bm25 = (_score / max_score) * 100;
                    return Math.min(100, normalized_bm25);
                """,
                "params": {
                    "max_bm25_score": max_score
                }
            }
        }})


