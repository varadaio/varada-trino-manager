from .connections import Trino
from .configuration import Connection
from .utils import logger
from click import exceptions
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from json import load, dumps
from pathlib import Path
from random import choice
from typing import Tuple


overall_res = defaultdict(lambda: defaultdict(dict))


def run_queries(serial_queries: dict, client: Trino, workload: int = 1) -> Tuple[dict, int]:
    q_series_results = {}
    for query in serial_queries:
        _, stats = client.execute(query=serial_queries[query])
        q_series_results[query] = {"queryName": query, "queryId": stats["queryId"],
                                   "elapsedTime": round(stats["elapsedTimeMillis"]*0.001, 3),
                                   "cpuTime": round(stats["cpuTimeMillis"]*0.001, 3)}
        logger.info(f'Query: {query} QueryId: {stats["queryId"]} '
                    f'Single query execution time: {round(stats["elapsedTimeMillis"]*0.001, 3)} Seconds')
    return q_series_results, workload


def run(user: str, jsonpath: Path, concurrency: int, random: bool, iterations: int, queries_list: list, con: Connection):
    try:
        with open(jsonpath) as fd:
            queries = load(fd)
    except Exception:
        logger.exception(f'Failed reading {jsonpath}')
        raise exceptions.Exit(code=1)

    # Validate all query ids found in JSON
    if queries_list:
        logger.info(f'Run the following series of queries in parallel, for {iterations} iterations overall concurrency: {len(queries_list)*concurrency}:')
        for parallel_queries in queries_list:
            for qid in parallel_queries:
                if qid not in queries:
                    logger.error(f'Query number {qid} from list is not in {queries.keys()}')
                    raise exceptions.Exit(code=1)
            logger.info(f'Series {queries_list.index(parallel_queries)}: {parallel_queries}')

    with Trino(con=con, username=user) as client:
        for iteration in range(iterations):
            logger.info(f"Running: Iteration {iteration+1}")
            futures = []
            with ProcessPoolExecutor(max_workers=len(queries_list) if queries_list else concurrency) as executor:
                if random:
                    queries_to_run = [choice(list(queries.keys())) for _ in range(concurrency)]
                    logger.info(f'Randomly selected queries: {queries_to_run}, concurrency {concurrency}')
                    for query_id in queries_to_run:
                        futures.append(executor.submit(run_queries, {query_id: f'--{query_id}\nEXPLAIN ANALYZE {queries[query_id]}'}, client))
                else:
                    for _ in range(concurrency):
                        for series in queries_list:
                            serial_queries = {query_id: f'--{query_id}\nEXPLAIN ANALYZE {queries[query_id]}' for query_id in series}
                            futures.append(executor.submit(run_queries, serial_queries, client, queries_list.index(series)+1))

            queries_done = 0
            total_elapsed_time = 0
            for future in as_completed(futures):
                query_res, workload = future.result()
                for query_name, data in query_res.items():
                    if query_res[query_name]:
                        queries_done += 1
                        overall_res[f'iteration{iteration+1}'][f'workload{workload}'][f'{query_name}'] = data
                        total_elapsed_time += data["elapsedTime"]
                        logger.info(f'Query {query_name} elapsed time is {data["elapsedTime"]} Seconds, cpu time is {data["cpuTime"]} Seconds')

            logger.info(f'Iteration {iteration+1} average elapsed time is {total_elapsed_time / queries_done} Seconds')
            logger.info(f'Iteration {iteration+1} total elapsed time is {total_elapsed_time} Seconds')
        logger.info(f'Overall run results: {dumps(overall_res, indent=2)}')
