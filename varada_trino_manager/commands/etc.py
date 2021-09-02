from json import dump
from ..infra.constants import Paths
from ..infra.configuration import get_config
from ..infra.rest_commands import RestCommands, ExtendedRest
from click import group, argument, echo, option, Path as ClickPath
from ..infra.options import add_options, TARGET_MAP, NODES_OPTIONS
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
@add_options(NODES_OPTIONS)
@etc.command()
def jstack(target, destination_dir):
    """
    Collect jstack from the nodes and save to --destination-dir
    example: coordinator/node-1/node-2.../all
    default: all
    """
    coordinator, workers = TARGET_MAP[target]
    dir_path = Paths.logs_path if destination_dir is None else destination_dir
    jstack_results = parallel_rest_execute(rest_client_type=ExtendedRest, func=RestCommands.jstack, coordinator=coordinator, workers=workers)
    for future, hostname in jstack_results:
        with open(f'{dir_path}/jstack_{hostname}.json', 'w') as fd:
            dump(future.result(), fd, indent=2)


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
