from .connections import APIClient
from .configuration import Connection


class WarmJmx:
    SCHEDULED = 0
    STARTED = 1
    FINISHED = 2
    FAILED = 3
    SKIPPED_QUEUE_SIZE = 4
    SKIPPED_DEMOTER = 5
    WARM_JMX_Q = 'select sum(warm_scheduled) as warm_scheduled, ' \
                 'sum(warm_started) as warm_started, ' \
                 'sum(warm_finished) as warm_finished, ' \
                 'sum(warm_failed) as warm_failed, ' \
                 'sum(warm_skipped_due_queue_size) as warm_skipped_due_queue_size, ' \
                 'sum(warm_skipped_due_demoter) as warm_skipped_due_demoter ' \
                 'from jmx.current.\"io.varada.presto:type=VaradaStatsWarmingService,name=warming-service.varada\"'

    @staticmethod
    def get_warmup_status(con: Connection) -> list:
        with APIClient(con=con,) as presto_client:
            warm_status, _ = presto_client.execute(WarmJmx.WARM_JMX_Q)
            # since the returned value is always one line, we'll pop it to not have to ref index each time
            warm_status = warm_status.pop()
        return warm_status


class ExtVrdJmx:
    VARADA_MATCH_COLUMNS = 0
    VARADA_COLLECT_COLUMNS = 1
    EXTERNAL_MATCH_COLUMNS = 2
    EXTERNAL_COLLECT_COLUMNS = 3
    PREFILLED_COLLECT_COLUMNS = 4
    DISPATCHER_JMX_Q = "select " \
                       "sum(varada_match_columns) varada_match_columns, " \
                       "sum(varada_collect_columns) varada_collect_columns, " \
                       "sum(external_match_columns) external_match_columns, " \
                       "sum(external_collect_columns) external_collect_columns, " \
                       "sum(prefilled_collect_columns) prefilled_collect_columns " \
                       "from jmx.current.\"io.varada.presto:type=VaradaStatsDispatcherPageSource,name=dispatcherPageSource.varada\" " \
                       "group by 'group'"
