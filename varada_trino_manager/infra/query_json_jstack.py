from json import load
from time import sleep
from pathlib import Path
from .utils import logger
from click import exceptions
from threading import Thread, Event
from .configuration import Connection
from .rest_commands import RestCommands
from .connections import APIClient, VaradaRest
from .remote import parallel_ssh_execute, parallel_download, parallel_rest_execute


def run_query(query: str, client: APIClient) -> dict:
    _, stats = client.execute(query=query)
    logger.info(f'Query: {query} QueryId: {stats["queryId"]} '
                f'Query execution time: {round(stats["elapsedTimeMillis"]*0.001, 3)} Seconds')
    return {"queryId": stats["queryId"], "elapsedTime": round(stats["elapsedTimeMillis"]*0.001, 3)}


def collect_jstack(wait: int, keep_running: Event):
    collect_commands = [
        "echo '#######################################    NEW JSTACK CAPTURE   #######################################' | sudo tee -a /tmp/jstacks/jstack.txt",
        "sudo jps | awk '/TrinoServer/ {print $1}' | sudo tee -a /tmp/jstacks/server.pid",
        "sudo jstack $(sudo jps | awk '/TrinoServer/ {print $1}') | sudo tee -a /tmp/jstacks/jstack.txt || true",
    ]
    while keep_running.is_set():
        parallel_ssh_execute(command="\n".join(collect_commands))
        sleep(wait)


def run(user: str, con: Connection, jsonpath: Path, query: str, jstack_wait: int, dest_dir: str, session_properties: dict = None):
    try:
        with open(jsonpath) as fd:
            queries = load(fd)
    except Exception:
        logger.exception(f'Failed reading {jsonpath}')
        raise exceptions.Exit(code=1)

    if query not in queries:
        logger.error(f'Query {query} is not in {queries.keys()}')
        raise exceptions.Exit(code=1)

    dir_commands = [
        "sudo rm -rf /tmp/jstacks",
        "sudo rm -rf /tmp/jstacks.tar.gz",
        "sudo mkdir /tmp/jstacks",
    ]
    parallel_ssh_execute(command="\n".join(dir_commands))

    with APIClient(con=con, username=user, session_properties=session_properties) as trino_client:
        # Start collecting jstack as Thread, then run query; once query has completed - stop collection
        logger.info(f"Start collecting jstacks, interval of {jstack_wait}Sec")
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg="VTM Query JSON JStack: Start Jstack Collection")
        keep_collecting_jstack = Event()
        keep_collecting_jstack.set()
        collect = Thread(target=collect_jstack, args=(jstack_wait, keep_collecting_jstack))
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

    # download jstack collection
    logger.info(f"Downloading jstacks to {dest_dir}/")
    tar_commands = [
        "sudo tar -C /tmp/jstacks -zcf /tmp/jstacks.tar.gz .",
        "sudo chmod 777 /tmp/jstacks.tar.gz",
    ]
    parallel_ssh_execute(command="\n".join(tar_commands))

    parallel_download(
        remote_file_path="/tmp/jstacks.tar.gz", local_dir_path=dest_dir
    )
    logger.info(f'Getting query json for query_id {stats["queryId"]}, saving to {dest_dir}/')
    RestCommands.save_query_json(con=con, dest_dir=dest_dir, query_id=stats["queryId"])
