import json
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import trino
from traceback import format_exc
from time import sleep

empty_query = 'SET SESSION varada.empty_query='
warmup_jmx_q = 'select sum(warm_scheduled) as warm_scheduled, ' \
               'sum(warm_finished) as warm_finished, ' \
               'sum(warm_failed) as warm_failed, ' \
               'sum(warm_skipped_due_queue_size) as warm_skipped_due_queue_size, ' \
               'sum(warm_skipped_due_demoter) as warm_skipped_due_demoter ' \
               'from jmx.current.\"io.varada.presto:type=VaradaStatsWarmingService,name=warming-service.varada\"'


class PrestoClient:
    def __init__(self, host: str, port: int, user: str, catalog: str, schema: str):
        self.client = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            session_properties=dict()
        )

    def execute(self, query: str, fetch_all: bool = True) -> list:
        try:
            print(f'Executing: {query.encode()}')
            with self.client as con:
                cursor = con.cursor()
                cursor.execute(query)
                if fetch_all:
                    result = cursor.fetchall()
                else:
                    result = cursor.fetchone()
            return result, cursor.stats
        except Exception as e:
            print("Failed to execute query")
            raise e

    def set_session(self, key: str, value) -> None:
        if isinstance(value, str):
            value = f"'{value}'"  # quote strings.
        self.execute(f"SET SESSION {key}={value}")

    def reset_session(self, key: str) -> None:
        self.execute(f"RESET SESSION {key}")


def get_presto_client(coordinator_url: str, user: str) -> Optional[PrestoClient]:
    parsed_url = urlparse(coordinator_url)
    if None in (parsed_url.hostname, parsed_url.port):
        print(f'Invalid coordinator url {coordinator_url}')
        return None
    try:
        return PrestoClient(host=parsed_url.hostname,
                            port=parsed_url.port,
                            user=user,
                            catalog='system',   # This catalog always exists
                            schema='runtime')
    except Exception:
        print(f"Error creating presto client: {coordinator_url}")
        print(format_exc())
        return None


def run_query(query: str, presto_client: PrestoClient):
    res, stats = presto_client.execute(query)
    return res


def run_warmup_query(warm_query: str, presto_client: PrestoClient):
    # set empty query for faster warm query
    presto_client.set_session("varada.empty_query", True)
    run_query(warm_query, presto_client)
    # reset empty query
    presto_client.reset_session("varada.empty_query")


def run(prestohost: str, user: str, jsonpath: Path):
    presto_client = get_presto_client(prestohost, user)
    if not presto_client:
        print(f'Failed connecting to presto')

    try:
        with open(jsonpath) as fd:
            warmup_queries = json.load(fd)['warm_queries']

    except Exception:
        print(f'Failed reading {jsonpath}')
        print(format_exc())
        return

    # long warmup loop
    for warm_q in warmup_queries:
        warmup_complete = False
        while not warmup_complete:
            run_warmup_query(warm_query=warm_q, presto_client=presto_client)
            sleep(3)
            # warm_status:
            # [warm_scheduled, warm_finished,warm_failed, warm_skipped_due_queue_size, warm_skipped_due_demoter]
            warm_status = run_query(warmup_jmx_q, presto_client)[0]
            print(f'warm status: {warm_status}')
            while warm_status[0] - warm_status[2] - warm_status[3] - warm_status[4] != warm_status[1]:
                print(f'warm status: {warm_status}, check again in 1 min')
                sleep(60)
                warm_status = run_query(warmup_jmx_q, presto_client)[0]
            print(f'warm_scheduled: {warm_status[0]} - warm_skipped: {warm_status[2] + warm_status[3] + warm_status[4]} '
                     f'eq warm_finished: {warm_status[1]}')
            print(f'warmup iteration complete, checking no additional warmup needed§')
            run_warmup_query(warm_query=warm_q, presto_client=presto_client)
            sleep(5)
            new_warm_status = run_query(warmup_jmx_q, presto_client)[0]
            if new_warm_status[0] == warm_status[0]:
                print(f'warm_scheduled still: {new_warm_status[0]}, no additional warmup needed')
                warmup_complete = True
