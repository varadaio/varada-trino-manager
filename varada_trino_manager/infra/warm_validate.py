from time import sleep
from json import dumps
from pathlib import Path
from .utils import logger
from traceback import format_exc
from dataclasses import dataclass
from click import exceptions, echo
from .utils import read_file_as_json
from .configuration import Connection
from .connections import VaradaRest, Trino


@dataclass
class WarmJmx:
    scheduled: int
    started: int
    finished: int
    failed: int
    skipped_queue_size: int
    skipped_demoter: int

    def verify(self) -> bool:
        return self.scheduled - self.failed - self.skipped_demoter == self.finished


class ExtendedTrino(Trino):

    EMPTY_QUERY = "varada.empty_query"
    WARM_JMX_QUERY = (
        "select sum(warm_scheduled) as warm_scheduled, "
        "sum(warm_started) as warm_started, "
        "sum(warm_finished) as warm_finished, "
        "sum(warm_failed) as warm_failed, "
        "sum(warm_skipped_due_queue_size) as warm_skipped_due_queue_size, "
        "sum(warm_skipped_due_demoter) as warm_skipped_due_demoter "
        'from jmx.current."io.varada.presto:type=VaradaStatsWarmingService,name=warming-service.varada"'
    )

    def __init__(self, con: Connection, username: str, http_schema: str):
        super().__init__(
            con,
            username=username,
            http_schema=http_schema,
            session_properties={self.EMPTY_QUERY: True},
        )

    def check_warmup_status(self, verify_started: bool = False) -> bool:
        warm_status, _ = self.execute(self.WARM_JMX_QUERY)
        # since the returned value is always one line, we'll pop it to not have to ref index each time
        warm_status = WarmJmx(*warm_status.pop())
        logger.info(f"warm status: {warm_status}")
        if verify_started:
            logger.info(f"Check increment in 15 sec")
            sleep(15)
            new_warm_status_list, _ = self.execute(self.WARM_JMX_QUERY)
            new_warm_status = WarmJmx(*new_warm_status_list.pop())
            logger.info(f"warm status: {new_warm_status}")
            return (
                new_warm_status.verify()
                and new_warm_status.started == warm_status.started
            )
        return warm_status.verify()


def run(user: str, jsonpath: Path, con: Connection):
    try:
        warmup_queries = read_file_as_json(jsonpath)["warm_queries"]
    except Exception as e:
        if e is not FileNotFoundError:
            logger.error(f"Failed reading {jsonpath}")
            logger.error(format_exc())
            raise exceptions.Exit(code=1)
        raise

    with VaradaRest(con=con) as varada_rest, ExtendedTrino(
        con=con, username=user
    ) as presto_client:
        # long warmup loop - verify warmup query
        logger.info("Running warmup queries with varada.empty_query=true")
        for warm_q in warmup_queries:
            warmup_complete = False
            presto_client.execute(warm_q)
            sleep(3)
            while not warmup_complete:
                while not presto_client.check_warmup_status():
                    logger.info(f"Warmup in progress, check again in 1 min")
                    sleep(60)
                logger.info(f"warm_scheduled - warm_skipped eq warm_finished")
                logger.info(
                    f"Warmup iteration complete, verifying no additional warmup needed"
                )
                presto_client.execute(warm_q)
                if not presto_client.check_warmup_status(verify_started=True):
                    logger.info(f"Additional warmup iteration in progress")
                else:
                    logger.info(
                        f"Warmup iteration complete, moving to next warmup query"
                    )
                    warmup_complete = True
            try:
                logger.info(f"row_group_count after warmup query: \n {warm_q}")
                data = varada_rest.row_group_count().json()
                echo(dumps(data, indent=2))
            except Exception:
                logger.error(f"Failed rest call to row_group_count")
                logger.error(format_exc())

        logger.info(f"Warmup complete")
