from time import sleep
from pathlib import Path
from .utils import logger
from json import load, dump
from click import exceptions
from datetime import datetime
from threading import Thread, Event
from .configuration import Connection
from .rest_commands import RestCommands
from .remote import parallel_rest_execute
from .connections import APIClient, VaradaRest, ExtendedRest


def run_query(query: str, client: APIClient) -> dict:
    _, stats = client.execute(query=query)
    logger.info(f'Query: {query} QueryId: {stats["queryId"]} '
                f'Query execution time: {round(stats["elapsedTimeMillis"]*0.001, 3)} Seconds')
    return {"queryId": stats["queryId"], "elapsedTime": round(stats["elapsedTimeMillis"]*0.001, 3)}


def collect_jstack(wait: int, keep_running: Event, destination_dir: Path):
    while keep_running.is_set():
        jstack_results = parallel_rest_execute(rest_client_type=ExtendedRest, func=RestCommands.jstack)
        for future, hostname in jstack_results:
            with open(f"{destination_dir}/jstack_{hostname}_{datetime.now().strftime('%H%M%S%f')}.json", 'w') as fd:
                dump(future.result(), fd, indent=2)
        sleep(wait)


def run(user: str, con: Connection, jsonpath: Path, query: str, jstack_wait: int, dest_dir: str, catalog: str,
        session_properties: dict = None):
    try:
        with open(jsonpath) as fd:
            queries = load(fd)
    except Exception:
        logger.exception(f'Failed reading {jsonpath}')
        raise exceptions.Exit(code=1)

    if query not in queries:
        logger.error(f'Query {query} is not in {queries.keys()}')
        raise exceptions.Exit(code=1)

    # Create separate local directory for jstacks and json
    results_dir = Path(f'{dest_dir}query_{query}_{datetime.now()}'.replace(' ', '_').replace(':', '-'))
    results_dir.mkdir()

    with APIClient(con=con, username=user, session_properties=session_properties, catalog=catalog) as trino_client:
        # Start collecting jstack as Thread, then run query; once query has completed - stop collection
        logger.info(f"Start collecting jstacks, interval of {jstack_wait}Sec, saving to {results_dir}")
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg="VTM Query JSON JStack: Start Jstack Collection")
        keep_collecting_jstack = Event()
        keep_collecting_jstack.set()
        collect = Thread(target=collect_jstack, args=(jstack_wait, keep_collecting_jstack, results_dir))
        collect.start()
        if session_properties:
            logger.info(f'Running query with session properties: {session_properties}')
        logger.info(f'Running query {query}')
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg=f"VTM Query JSON JStack: Run Query: {query}")
        _, stats = trino_client.execute(query=queries[query])

        logger.info("Query completed, stopping jstacks collection")
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg="VTM Query JSON JStack: Stop Jstack Collection")
        keep_collecting_jstack.clear()
        collect.join()

    # get query json
    logger.info(f'Getting query json for query_id {stats["queryId"]}, saving to {results_dir}/')
    RestCommands.save_query_json(con=con, dest_dir=results_dir, query_id=stats["queryId"])
