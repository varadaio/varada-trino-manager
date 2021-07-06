from time import sleep
from pathlib import Path
from typing import Tuple
from random import choice
from .utils import logger
from json import load, dumps
from .connections import Trino
from click import exceptions, echo
from collections import defaultdict
from .configuration import Connection
from concurrent.futures import ProcessPoolExecutor, as_completed

overall_res = defaultdict(lambda: defaultdict(dict))


def run_queries(serial_queries: dict, client: Trino, workload: int = 1, return_res: bool = False) -> Tuple[dict, int, list]:
    q_series_results = {}
    for query in serial_queries:
        q_res, q_stats = client.execute(query=serial_queries[query])
        q_series_results[query] = {"queryName": query, "queryId": q_stats["queryId"],
                                   "elapsedTime": round(q_stats["elapsedTimeMillis"] * 0.001, 3),
                                   "cpuTime": round(q_stats["cpuTimeMillis"] * 0.001, 3),
                                   "processedRows": q_stats["processedRows"],
                                   "processedBytes": q_stats["processedBytes"],
                                   "totalSplits": q_stats["totalSplits"]}
        logger.info(f'Query: {query} QueryId: {q_stats["queryId"]} '
                    f'Single query execution time: {round(q_stats["elapsedTimeMillis"] * 0.001, 3)} Seconds')
    return q_series_results, workload, q_res if return_res else None


def run(user: str, jsonpath: Path, concurrency: int, random: bool, iterations: int, sleep_time: int, queries_list: list,
        con: Connection, get_results: bool = False, session_properties: dict = None):
    try:
        with open(jsonpath) as fd:
            queries = load(fd)
    except Exception:
        logger.exception(f'Failed reading {jsonpath}')
        raise exceptions.Exit(code=1)

    # Validate all query ids found in JSON
    if queries_list:
        if session_properties:
            logger.info(f'Running with session properties: {session_properties}')
        logger.info(f'Run the following series of queries in parallel, for {iterations} iterations overall concurrency:'
                    f' {len(queries_list)*concurrency}:')
        for parallel_queries in queries_list:
            for qid in parallel_queries:
                if qid not in queries:
                    logger.error(f'Query number {qid} from list is not in {queries.keys()}')
                    raise exceptions.Exit(code=1)
            logger.info(f'Series {queries_list.index(parallel_queries)}: {parallel_queries}')

    with Trino(con=con, username=user, session_properties=session_properties) as client:
        for iteration in range(iterations):
            logger.info(f"Running: Iteration {iteration+1}")
            futures = []
            with ProcessPoolExecutor(max_workers=len(queries_list) if queries_list else concurrency) as executor:
                if random:
                    queries_to_run = [choice(list(queries.keys())) for _ in range(concurrency)]
                    logger.info(f'Randomly selected queries: {queries_to_run}, concurrency {concurrency}')
                    for query_id in queries_to_run:
                        futures.append(executor.submit(run_queries,
                                                       {query_id: f'--{query_id}\n {queries[query_id]}' if get_results
                                                        else f'--{query_id}\nEXPLAIN ANALYZE {queries[query_id]}'},
                                                       client,
                                                       get_results,
                                                       get_results))
                else:
                    for _ in range(concurrency):
                        for series in queries_list:
                            serial_queries = {query_id: f'--{query_id}\n {queries[query_id]}' for query_id in series} \
                                if get_results else \
                                {query_id: f'--{query_id}\nEXPLAIN ANALYZE {queries[query_id]}' for query_id in series}
                            futures.append(executor.submit(run_queries,
                                                           serial_queries,
                                                           client,
                                                           queries_list.index(series) + 1,
                                                           get_results))

            queries_done = 0
            total_elapsed_time = 0
            for future in as_completed(futures):
                query_stats, workload, query_results = future.result()
                for query_name, data in query_stats.items():
                    if query_stats[query_name]:
                        queries_done += 1
                        overall_res[f'iteration{iteration+1}'][f'workload{workload}'][f'{query_name}'] = data
                        total_elapsed_time += data["elapsedTime"]
                        logger.info(f'Query {query_name} elapsed time is {data["elapsedTime"]} Seconds, cpu time is {data["cpuTime"]} Seconds')
                        if query_results:
                            echo(f'Query {query_name} results:\n {query_results}')

            logger.info(f'Iteration {iteration+1} average elapsed time is {total_elapsed_time / queries_done} Seconds')
            logger.info(f'Iteration {iteration+1} total elapsed time is {total_elapsed_time} Seconds')
            if sleep_time and iteration < iterations:
                logger.info(f'Sleeping {sleep_time} seconds before next run')
                sleep(sleep_time)
        logger.info(f'Overall run results: {dumps(overall_res, indent=2)}')
