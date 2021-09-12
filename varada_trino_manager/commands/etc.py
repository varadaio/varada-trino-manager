from json import dump
from ..infra.jmx import WarmJmx
from ..infra.utils import logger
from ..infra.constants import Paths
from ..infra.configuration import get_config
from ..infra.rest_commands import RestCommands, ExtendedRest
from click import group, argument, echo, option, Path as ClickPath
from ..infra.remote import parallel_ssh_execute, rest_execute, parallel_rest_execute


@group()
def etc():
    """
    More utilities
    """
    pass


@argument("node", default="coordinator", nargs=1)
@etc.command()
def info(node):
    """
    Access v1/info of selected node
    example: coordinator/node-1/node-2
    default: coordinator
    """
    con = get_config().get_connection_by_name(node)
    echo(rest_execute(con=con, rest_client_type=ExtendedRest, func=RestCommands.info))


@option(
    "-d",
    "--destination-dir",
    type=ClickPath(),
    default=None,
    help="Destination dir to save the jstack output",
)
@argument("target", default="all", nargs=1)
@etc.command()
def jstack(target, destination_dir):
    """
    Collect jstack from the nodes and save to --destination-dir
    example: coordinator/node-1/node-2...
    default: all (no target given)
    """
    con = get_config().get_connection_by_name(target) if target != "all" else None
    dir_path = Paths.logs_path if destination_dir is None else destination_dir
    jstack_results = parallel_rest_execute(rest_client_type=ExtendedRest, func=RestCommands.jstack) if target == "all" \
        else [(rest_execute(con=con, rest_client_type=ExtendedRest, func=RestCommands.jstack), con.hostname)]
    for future, hostname in jstack_results:
        with open(f'{dir_path}/jstack_{hostname}.json', 'w') as fd:
            if target == "all":
                dump(future.result(), fd, indent=2)
            else:
                dump(future, fd, indent=2)


@etc.command()
def is_panic_error():
    """
    Verify if a node is in panic or has errors in launcher.log
    """
    command = "tail -n 30 /var/log/presto/launcher.log | grep PANIC | wc -l"
    tasks = parallel_ssh_execute(command=command)
    for panic, hostname in tasks:
        if bool(int(panic.result().strip())):
            echo(f"found panic in {hostname}")
        else:
            echo(f"no panic found in {hostname}")
    command = "tail -n 30 /var/log/presto/launcher.log | grep ERROR | wc -l"
    tasks = parallel_ssh_execute(command=command)
    for panic, hostname in tasks:
        if bool(int(panic.result().strip())):
            echo(f"found error in {hostname}")
        else:
            echo(f"no error found in {hostname}")


@etc.command()
def loading_status():
    """
    Print Varada loading counters
    """
    con = get_config().get_connection_by_name("coordinator")
    status = WarmJmx.get_warmup_status(con=con)
    logger.info(f"Loading Status: \n"
                f"warm_scheduled: {status[WarmJmx.SCHEDULED]}\n"
                f"warm_started: {status[WarmJmx.STARTED]}\n"
                f"warm_finished: {status[WarmJmx.FINISHED]}\n"
                f"warm_failed: {status[WarmJmx.FAILED]}\n"
                f"warm_skipped_due_queue_size: {status[WarmJmx.SKIPPED_QUEUE_SIZE]}\n"
                f"warm_skipped_due_demoter: {status[WarmJmx.SKIPPED_DEMOTER]}\n")
