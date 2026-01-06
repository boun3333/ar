from elasticsearch import AsyncElasticsearch, NotFoundError, ConnectionTimeout, TransportError
from elasticsearch.helpers import async_scan
from utils.logger import get_logger
from utils.config import REPO_HOST, REPO_USER,REPO_PASS

class AESClient:
    def __init__(self):
        self.logger = get_logger("stdout")
        self._open_connection()

    def _open_connection(self):
        hosts = REPO_HOST.split(",")
        basic_auth = (REPO_USER, REPO_PASS)
        self.es = AsyncElasticsearch(hosts=hosts, verify_certs=False, basic_auth=basic_auth, connections_per_node=10)

    async def _close_connection(self):
        await self.es.close()

    async def _check_connection(self):
        try:
            is_alive = await self.es.ping()
            if not is_alive:
                self.logger.info("[REPO] Elasticsearch Ping Faild. Reopning connection...")
                self._open_connection()
        except (ConnectionTimeout, TransportError) as e:
            self.logger.error(f"[REPO] Connection Error During Ping : {e}. Reopning connection...")
            self._open_connection()
        except Exception as e:
            self.logger.error(f"[REPO] Connection Ping Exception : {e}", exc_info=True)
            self._open_connection()

    async def get_count(self, index: str, query: dict):
        try:
            data = await self.es.count(index=index, body=query)
            cnt = data['count']
            return cnt
        except NotFoundError:
            return 0

    async def create(self, index: str, mapping: dict = None):
        return await self.es.indices.create(index=index, body=mapping)

    async def insert(self, index: str, id: str, document: dict):
        return await self.es.index(index=index, id=id, document=document, refresh='wait_for')

    async def exists_index(self, index: str):
        return await self.es.indices.exists(index=index)

    async def get_source(self, index:str, id:str):
        try:
            res = await self.es.get_source(index=index, id=id)
            return res
        except NotFoundError:
            return None
        except Exception as e:
            self.logger.error(f"[REPO] Connection GetSource Exception : {e}", exc_info=True)
            self._open_connection()

    async def search(self, index: str, query: dict, search_type='hits', _all=False):
        """
        Elasticsearch search 쿼리 실행
        :param index: 인덱스명
        :param query: 검색 쿼리
        :param search_type: 검색 유형 (hits: 일반 / agg: 통계(aggregations))
        :param _all : 조회된 데이터 전체를 가져올지 여부
        :return: 딕셔너리 형태의 데이터 리턴 (최대 10000개, 그 이상은 scan 함수 이용)
        """
        try:
            res = await self.es.search(index=index, body=query)

            if search_type == 'hits' and _all:
                return res['hits']['hits']
            elif search_type == 'hits':
                for r in res['hits']['hits']:
                    return r['_source']
            elif search_type == 'agg':
                return res['aggregations']
            elif search_type == 'all':
                return res
            else:
                raise ValueError(f'[REPO] search_type: "{search_type}" is not supported.')

        except Exception as e:
            self.logger.error(f"[REPO] Connection Search Exception : {e}", exc_info=True)
            self._open_connection()

    async def scan(self, index: str, query: dict, _source=True):
        """
        Elasticsearch search 쿼리를 스크롤 방식으로 실행 (10000개 이상 데이터 조회 시 사용)
        Generator 형식
        :param index: 인덱스명
        :param query: 검색 쿼리
        """
        try:
            if _source:
                async for res in async_scan(client=self.es, index=index, query=query, preserve_order=True):
                    yield res['_source']
            else:
                async for res in async_scan(client=self.es, index=index, query=query, preserve_order=True):
                    yield res
        except NotFoundError:
            raise StopAsyncIteration("[REPO] No data found for the given query.")

    async def delete_by_query(self, index:str, query:dict):
        try:
            res = await self.es.delete_by_query(index=index, query=query)
        except NotFoundError:
            raise StopAsyncIteration("[REPO] No data found for the given query.")