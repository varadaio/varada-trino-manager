from time import sleep
from json import dumps
from pathlib import Path
from .utils import logger
from traceback import format_exc
from click import exceptions, echo
from .utils import read_file_as_json
from .configuration import Connection
from .remote import parallel_rest_execute
from .connections import VaradaRest, APIClient
from ..infra.rest_commands import RestCommands


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


def check_warmup_status(presto_client: APIClient, verify_started: bool = False) -> bool:
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


def run(user: str, jsonpath: Path, con: Connection, queries_list: list):
    try:
        warmup_queries = read_file_as_json(jsonpath)
    except Exception as e:
        if e is not FileNotFoundError:
            logger.error(f'Failed reading {jsonpath}')
            logger.error(format_exc())
            raise exceptions.Exit(code=1)
        raise

    if len(queries_list) > 1:
        logger.error(f'Please list queries as comma separated, without spaces')
        raise exceptions.Exit(code=1)
    # queries_list coming in as list of lists, only first item relevant
    logger.info(f'Running warm-and-validate with queries: {queries_list[0]}')
    for qid in queries_list[0]:
        if qid not in warmup_queries:
            logger.error(f'Query {qid} from list is not in {warmup_queries.keys()}')
            raise exceptions.Exit(code=1)

    with VaradaRest(con=con) as varada_rest, APIClient(con=con, username=user, session_properties={EMPTY_Q: 'true'}) as presto_client:
        # long warmup loop - verify warmup query
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg="VTM Warm And Validate: Start")
        logger.info('Running warmup queries with varada.empty_query=true')
        for warm_q in warmup_queries:
            warmup_complete = False
            parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg=f"VTM Warm And Validate: Running query: {warm_q}")
            presto_client.execute(warmup_queries[warm_q])
            sleep(3)
            while not warmup_complete:
                while not check_warmup_status(presto_client=presto_client):
                    logger.info(f'Warmup in progress, check again in 1 min')
                    sleep(60)
                logger.info(f'warm_scheduled - warm_skipped eq warm_finished')
                logger.info(f'Warmup iteration complete, verifying no additional warmup needed')
                presto_client.execute(warmup_queries[warm_q])
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
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg="VTM Warm And Validate: End")
        logger.info(f'Warmup complete')
