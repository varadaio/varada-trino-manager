from json import load
from .utils import logger
from .configuration import Connection
from .rest_commands import RestCommands
from dataclasses import dataclass


def zero_dev(func):
    def zero_dev_deco(self):
        try:
            return func(self)
        except ZeroDivisionError:
            pass
    return zero_dev_deco


@dataclass
class QueryAcceleration:
    externalMatch: int
    externalCollect: int
    varadaMatch: int
    varadaCollect: int

    @property
    @zero_dev
    def overall(self):
        return round((self.varadaCollect + self.varadaMatch) * 100 / (self.varadaCollect + self.varadaMatch + self.externalCollect + self.externalMatch), 3)

    @property
    @zero_dev
    def filtering(self):
        return round(self.varadaMatch * 100 / (self.varadaMatch + self.externalMatch), 3)

    @property
    @zero_dev
    def projection(self):
        return round(self.varadaCollect * 100 / (self.varadaCollect + self.externalCollect), 3)

    def __iter__(self):
        for value in self.__dict__.values():
            yield value


def run(con: Connection, results_dir: str, query_id: str):
    # dict for aggregated custom metrics, map of key names in query.json
    agg_metrics = {
        'externalMatch': 0,
        'externalCollect': 0,
        'varadaMatch': 0,
        'varadaCollect': 0
    }
    key_map = {
        'dispatcherPageSource:external_match_columns': 'externalMatch',
        'dispatcherPageSource:external_collect_columns': 'externalCollect',
        'dispatcherPageSource:varada_match_columns': 'varadaMatch',
        'dispatcherPageSource:varada_collect_columns': 'varadaCollect',
        'dispatcherPageSource:prefilled_collect_columns': 'varadaCollect'
    }
    # get query json
    logger.info(f'Getting query json for query_id {query_id}, saving to {results_dir}')
    RestCommands.save_query_json(con=con, dest_dir=results_dir, query_id=query_id)

    # get data from json, filter only custom metrics in operator summaries
    with open(f'{results_dir}/{query_id}.json', 'r') as f:
        query_json = load(f)
    operator_summaries = query_json['queryStats']['operatorSummaries']
    metrics = []
    for op_sum in operator_summaries:
        if 'connectorMetrics' in op_sum.keys():
            metrics.append(op_sum['connectorMetrics'])
        elif 'metrics' in op_sum.keys():
            metrics.append(op_sum['metrics'])

    # sum of all flat custom metrics
    for metric in metrics:
        for key_name in key_map.keys():
            if key_name in metric.keys():
                agg_metrics[key_map[key_name]] += metric[key_name]['total']

    # calculate acceleration
    logger.info(f'Varada counters for query {query_id}: \n {agg_metrics}')
    query_acceleration = QueryAcceleration(externalMatch=agg_metrics['externalMatch'],
                                           externalCollect=agg_metrics['externalCollect'],
                                           varadaMatch=agg_metrics['varadaMatch'],
                                           varadaCollect=agg_metrics['varadaCollect'])

    logger.info(f'Query {query_id}:\n'
                f'Overall Acceleration %: {query_acceleration.overall}\n'
                f'Filtering Acceleration %: {query_acceleration.filtering}\n'
                f'Projection Acceleration %: {query_acceleration.projection}')
