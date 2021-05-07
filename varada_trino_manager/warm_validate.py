from click import exceptions, echo
from json import load, dumps
from pathlib import Path
from time import sleep
from traceback import format_exc
from trino.dbapi import connect
from typing import Optional, Tuple
from urllib.parse import urlparse
from .configuration import Connection
from .utils import logger
from .connections import VaradaRest


class WarmJmx:
    SCHEDULED = 0
    STARTED = 1
    FINISHED = 2
    FAILED = 3
    SKIPPED_QUEUE_SIZE = 4
    SKIPPED_DEMOTER = 5


EMPTY_Q = 'varada.empty_query'
WARM_JMX_Q = 'select sum(warm_scheduled) as warm_scheduled, ' \
             'sum(warm_started) as warm_started, ' \
             'sum(warm_finished) as warm_finished, ' \
             'sum(warm_failed) as warm_failed, ' \
             'sum(warm_skipped_due_queue_size) as warm_skipped_due_queue_size, ' \
             'sum(warm_skipped_due_demoter) as warm_skipped_due_demoter ' \
             'from jmx.current.\"io.varada.presto:type=VaradaStatsWarmingService,name=warming-service.varada\"'


class PrestoClient:
    def __init__(self, host: str, port: int, user: str, catalog: str, schema: str):
        self.client = connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            session_properties=dict()
        )

    def execute(self, query: str, fetch_all: bool = True) -> Tuple[list, dict]:
        try:
            logger.info(f'Executing: {query.encode()}')
            with self.client as con:
                cursor = con.cursor()
                cursor.execute(query)
                result =cursor.fetchall() if fetch_all else cursor.fetchone()
            return result, cursor.stats
        except Exception as e:
            logger.error(f'Failed to execute query, reason: {e}')
            raise exceptions.Exit(code=1)

    def set_session(self, key: str, value) -> None:
        value = f"'{value}'" if isinstance(value, str) else value
        self.execute(f"SET SESSION {key}={value}")

    def reset_session(self, key: str) -> None:
        self.execute(f"RESET SESSION {key}")


def get_presto_client(coordinator_url: str, user: str) -> Optional[PrestoClient]:
    parsed_url = urlparse(coordinator_url)
    if None in (parsed_url.hostname, parsed_url.port):
        logger.error(f'Invalid coordinator url {coordinator_url}')
        raise exceptions.Exit(code=1)
    try:
        return PrestoClient(host=parsed_url.hostname,
                            port=parsed_url.port,
                            user=user,
                            catalog='system',   # This catalog always exists
                            schema='runtime')
    except Exception:
        logger.error(f'Error creating presto client: {coordinator_url}')
        logger.error(format_exc())
        raise exceptions.Exit(code=1)


def run_warmup_query(warm_query: str, presto_client: PrestoClient):
    # set empty query for faster warm query
    presto_client.set_session(EMPTY_Q, True)
    presto_client.execute(warm_query)
    presto_client.reset_session(EMPTY_Q)


def check_warmup_status(presto_client: PrestoClient, verify_started: bool = False) -> bool:
    warm_status, _ = presto_client.execute(WARM_JMX_Q)
    # since the returned value is always one line, we'll pop it to not have to ref index each time
    warm_status = warm_status.pop()
    logger.info(f'warm status: {warm_status}')
    if verify_started:
        logger.info(f'Check increment in 15 sec')
        sleep(15)
        new_warm_status, _ = presto_client.execute(WARM_JMX_Q)
        new_warm_status = new_warm_status.pop()
        logger.info(f'warm status: {new_warm_status}')
        return (new_warm_status[WarmJmx.SCHEDULED] - new_warm_status[WarmJmx.FAILED] - new_warm_status[WarmJmx.SKIPPED_DEMOTER]
                == new_warm_status[WarmJmx.FINISHED]) and new_warm_status[WarmJmx.STARTED] == warm_status[WarmJmx.STARTED]
    return warm_status[WarmJmx.SCHEDULED] - warm_status[WarmJmx.FAILED] - warm_status[WarmJmx.SKIPPED_DEMOTER] \
        == warm_status[WarmJmx.FINISHED]


def run(presto_host: str, user: str, jsonpath: Path, con: Connection):
    with VaradaRest(con=con) as varada_rest:
        presto_client = get_presto_client(presto_host, user)
        if not presto_client:
            logger.error(f'Failed connecting to presto host on coordinator')
            raise exceptions.Exit(code=1)
        try:
            with open(jsonpath) as fd:
                warmup_queries = load(fd)['warm_queries']
        except Exception:
            logger.error(f'Failed reading {jsonpath}')
            logger.error(format_exc())
            raise exceptions.Exit(code=1)

        # long warmup loop - verify warmup query
        for warm_q in warmup_queries:
            warmup_complete = False
            run_warmup_query(warm_query=warm_q, presto_client=presto_client)
            sleep(3)
            while not warmup_complete:
                while not check_warmup_status(presto_client=presto_client):
                    logger.info(f'Warmup in progress, check again in 1 min')
                    sleep(60)
                logger.info(f'warm_scheduled - warm_skipped eq warm_finished')
                logger.info(f'Warmup iteration complete, verifying no additional warmup needed')
                run_warmup_query(warm_query=warm_q, presto_client=presto_client)
                if not check_warmup_status(presto_client=presto_client, verify_started=True):
                    logger.info(f'Additional warmup iteration in progress')
                else:
                    logger.info(f'Warmup iteration complete, moving to next warmup query')
                    warmup_complete = True
            try:
                logger.info(f'row_group_count after warmup query: \n {warm_q}')
                data = varada_rest.row_group_count().json()
                echo(dumps(data, indent=2))
            except Exception:
                logger.error(f'Failed rest call to row_group_count')
                logger.error(format_exc())

        logger.info(f'Warmup complete')
