from time import sleep
from pathlib import Path
from typing import Tuple
from random import choice
from .utils import logger
from datetime import datetime
from json import load, dump, dumps
from .connections import APIClient
from click import exceptions, echo
from collections import defaultdict
from .connections import VaradaRest
from .configuration import Connection
from .remote import parallel_rest_execute
from ..infra.rest_commands import RestCommands
from concurrent.futures import ProcessPoolExecutor, as_completed

overall_res = defaultdict(lambda: defaultdict(list))


def run_queries(serial_queries: dict, client: APIClient, multiple_query: bool, workload: int = 1, return_res: bool = False,
                is_concurrent: bool = False, collect_query_json: bool = False, con: Connection = None,
                query_jsons_dir: Path = None, ) -> Tuple[list, int, list]:
    q_series_results = []
    for query in serial_queries:
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg=f"VTM Run Query: {query}")
        q_res, q_stats = client.execute(query=serial_queries[query])
        q_series_results.append({"queryName": query, "queryId": q_stats["queryId"],
                                 "elapsedTime": round(q_stats["elapsedTimeMillis"] * 0.001, 3),
                                 "cpuTime": round(q_stats["cpuTimeMillis"] * 0.001, 3),
                                 "processedRows": q_stats["processedRows"],
                                 "processedBytes": q_stats["processedBytes"],
                                 "totalSplits": q_stats["totalSplits"],
                                 "results": q_res[0:9] if (return_res and multiple_query) else None,
                                 })
        logger.info(f'Query: {query} QueryId: {q_stats["queryId"]} '
                    f'Single query execution time: {round(q_stats["elapsedTimeMillis"] * 0.001, 3)} Seconds')
        if collect_query_json:
            logger.info(f'Getting query json for query_id {q_stats["queryId"]}, saving to {query_jsons_dir}')
            RestCommands.save_query_json(con=con, dest_dir=query_jsons_dir, query_id=q_stats["queryId"])
    return q_series_results, workload, q_res if (
                not is_concurrent and not multiple_query and return_res) else None


def validate_queries_list(q_list: list, queries: dict):
    if not q_list:
        logger.error(f'No queries given to run in queries_list {q_list}')
        raise exceptions.Exit(code=1)
    for parallel_queries in q_list:
        for qid in parallel_queries:
            if qid not in queries:
                logger.error(f'Query number {qid} from list is not in {queries.keys()}')
                raise exceptions.Exit(code=1)
        logger.info(f'Series {q_list.index(parallel_queries)}: {parallel_queries}')


def run_json(file_path: Path, concurrency: int, random: bool, queries_list: list, get_results: bool)\
        -> Tuple[list, int]:
    try:
        fd = open(file_path)
        queries = load(fd)
    except Exception as e:
        logger.exception(f"Failed to open {file_path}: {e}")
        raise exceptions.Exit(code=1)
    concurrency_factor = (len(queries_list) if queries_list else 1) if not random else concurrency
    if random:
        # Randomly choose queries from file
        query_names_to_run = [choice(list(queries.keys())) for _ in range(concurrency_factor)]
        logger.info(f'Randomly selected: {query_names_to_run}, concurrency {concurrency_factor}')
        queries_to_run = [[{query_name: f'--{query_name}\n {queries[query_name]}' if get_results else
                                        f'--{query_name}\n EXPLAIN ANALYZE {queries[query_name]}'}
                           for query_name in query_names_to_run]]
    elif queries_list:
        # Validate and prepare queries from queries_list
        validate_queries_list(queries_list, queries)
        queries_to_run = []
        for series in queries_list:
            serial_queries = [{query_name: f'--{query_name}\n {queries[query_name]}' if get_results else
                                           f'--{query_name}\n EXPLAIN ANALYZE {queries[query_name]}'}
                              for query_name in series]
            queries_to_run.append(serial_queries)
    else:
        # No queries_list provided -> Run all queries from file, serially
        queries_to_run = [[{query_name: f'--{query_name}\n {queries[query_name]}' if get_results else
                                        f'--{query_name}\n EXPLAIN ANALYZE {queries[query_name]}'}
                           for query_name in queries.keys()]]

    fd.close()
    return queries_to_run, concurrency_factor


def run_txt(file_path: Path, concurrency: int, random: bool, queries_list: list, get_results: bool) -> Tuple[list, int]:
    try:
        fd = open(file_path)
        queries = fd.read().split(";")
    except Exception as e:
        logger.exception(f"Failed to open {file_path}: {e}")
        raise exceptions.Exit(code=1)
    logger.info(f'Running queries from file: {file_path}')
    concurrency_factor = (len(queries_list) if queries_list else 1) if not random else concurrency
    if random:
        queries_to_run_indexes = [queries.index(choice(queries)) for _ in range(concurrency_factor)]
        queries_to_run = [[{query_number: f'--Query{query_number}\n {queries[query_number]}' if get_results else
                                          f'--Query{query_number}\n EXPLAIN ANALYZE {queries[query_number]}'}
                           for query_number in queries_to_run_indexes]]
        logger.info(f'Randomly selected queries number: {queries_to_run_indexes}, concurrency {concurrency_factor}')
    elif queries_list:
        # Run queries from file in given indices list, concurrency determined by space separated lists
        logger.info('Preparing to run the following queries:')
        queries_to_run = []
        for series in queries_list:
            serial_queries = [{query_number: f'--Query{query_number}\n {queries[int(query_number)]}' if get_results else
                                             f'--Query{query_number}\n EXPLAIN ANALYZE {queries[int(query_number)]}'}
                              for query_number in series]
            queries_to_run.append(serial_queries)
            logger.info(f'Series {queries_list.index(series)}: {series}')
    else:
        # No queries_list provided -> Run all queries from file, serially
        logger.info(f'No queries selected, running all {len(queries)} queries from file serially')
        queries_to_run = [[{query_number: f'--Query{query_number}\n {queries[query_number]}' if get_results else
                                          f'--{query_number}\n EXPLAIN ANALYZE {queries[query_number]}'}
                           for query_number in range(len(queries) - 1)]]
    fd.close()
    return queries_to_run, concurrency_factor


def run(user: str, jsonpath: Path, txtpath: Path, queries_list: list, concurrency: int, random: bool, iterations: int,
        sleep_time: int, con: Connection, catalog: str, destination_dir: Path, get_results: bool = False,
        session_properties: dict = None, collect_query_json: bool = False,):
    func_maps = {
        (False, True): (run_txt, txtpath),
        (True, False): (run_json, jsonpath)
    }
    if not (txtpath or jsonpath):
        logger.exception(f"Please specify either json file (-j) or txt file (-f) with queries")
        raise exceptions.Exit(code=1)
    if random:
        if queries_list:
            logger.exception(f"Random option (-r) cannot be run with queries_list argument")
            raise exceptions.Exit(code=1)
        elif not concurrency:
            logger.exception(f"Concurrency option (-c) must be specified with random option (-r)")
            raise exceptions.Exit(code=1)
    if collect_query_json:
        # Create separate local directory for jsons
        query_jsons_dir = Path(f'{destination_dir}_query_jsons_{datetime.now()}'.replace(' ', '_').replace(':', '-'))
        query_jsons_dir.mkdir()

    func, file_path = func_maps[(bool(jsonpath), bool(txtpath))]
    queries_prepared, verified_concurrency = func(file_path=file_path,
                                                  concurrency=concurrency,
                                                  random=random,
                                                  queries_list=queries_list,
                                                  get_results=get_results)
    q_lists_length_check = [len(q_lst) > 1 for q_lst in queries_prepared]
    multiple_query = True in q_lists_length_check or len(queries_prepared) > 1
    logger.info(f'Run the queries on catalog {catalog}, '
                f'for {iterations} iterations overall concurrency'
                f' {verified_concurrency}')
    if session_properties:
        logger.info(f'Running with session properties: {session_properties}')

    with APIClient(con=con, username=user, session_properties=session_properties, catalog=catalog) as client:
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg="VTM Query Runner Start")
        for iteration in range(iterations):
            logger.info(f"Running: Iteration {iteration + 1}")
            parallel_rest_execute(rest_client_type=VaradaRest,
                                  func=RestCommands.dev_log,
                                  msg=f"VTM Query Runner Iteration {iteration + 1}")
            futures = []
            with ProcessPoolExecutor(max_workers=verified_concurrency) as executor:
                for series in queries_prepared:
                    for query in series:
                        futures.append(executor.submit(run_queries,
                                                       query,
                                                       client,
                                                       multiple_query,
                                                       queries_prepared.index(series) + 1,
                                                       get_results,
                                                       verified_concurrency > 1,
                                                       collect_query_json,
                                                       con,
                                                       query_jsons_dir if collect_query_json else None,
                                                       ))
            queries_done = 0
            total_elapsed_time = 0
            for future in as_completed(futures):
                query_stats, workload, query_results = future.result()
                for query_data in query_stats:
                    queries_done += 1
                    overall_res[f'iteration{iteration + 1}'][f'workload{workload}'].append(query_data)
                    total_elapsed_time += query_data["elapsedTime"]
                    logger.info(
                        f'Query {query_data["queryName"]} elapsed time is {query_data["elapsedTime"]} Seconds, cpu time is {query_data["cpuTime"]} Seconds')
                    if query_results:
                        echo(f'Query {query_data["queryName"]} results:\n {query_results[0:9]}')
                        if get_results and len(query_results) > 10:
                            with open(f"{destination_dir}/query_{query_data['queryName']}_results_{datetime.now().strftime('%H%M%S%f')}.json", 'w') as fd:
                                echo(f'Query {query_data["queryName"]} full results saved to file {fd.name}')
                                dump(query_results, fd, indent=2)
                            fd.close()


            logger.info(
                f'Iteration {iteration + 1} average elapsed time is {total_elapsed_time / queries_done} Seconds')
            logger.info(f'Iteration {iteration + 1} total elapsed time is {total_elapsed_time} Seconds')
            if sleep_time and iteration < iterations:
                logger.info(f'Sleeping {sleep_time} seconds before next run')
                sleep(sleep_time)
        parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg="VTM Query Runner End")
        logger.info(f'Overall run results: {dumps(overall_res, indent=2)}')
        with open(f"{destination_dir}/query_runner_overall_results_{datetime.now().strftime('%H%M%S%f')}.json", 'w') as fd:
            dump(overall_res, fd, indent=2)
            echo(f'saving overall run results to: {fd.name}')
        fd.close()
