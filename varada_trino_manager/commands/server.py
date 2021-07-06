from click import group, echo
from ..infra.connections import Trino
from ..infra.configuration import get_config
from ..infra.rest_commands import RestCommands
from ..infra.remote import parallel_ssh_execute, rest_execute
from ..infra.options import add_options, TARGET_MAP, NODES_OPTIONS


@group()
def server():
    """
    Server management related commands
    """
    pass


@add_options(NODES_OPTIONS)
@server.command()
def stop(target: str):
    """
    Start presto service
    """
    run_func(command="sudo systemctl restart presto", target=target)


@add_options(NODES_OPTIONS)
@server.command()
def start(target: str):
    """
    Start presto service
    """
    run_func(command="sudo systemctl restart presto", target=target)


@add_options(NODES_OPTIONS)
@server.command()
def restart(target: str):
    """
    Restart presto service
    """
    run_func(command="sudo systemctl restart presto", target=target)


@server.command()
def status():
    """
    Checks if all nodes are connected
    """
    con = get_config().get_connection_by_name("coordinator")
    if not rest_execute(
        con=con, rest_client_type=Trino, func=RestCommands.is_all_nodes_connected
    ):
        echo("Not all nodes are connected")
    else:
        echo("All nodes are connected")


def run_func(command: str, target: str):
    coordinator, workers = TARGET_MAP[target]
    parallel_ssh_execute(command=command, coordinator=coordinator, workers=workers)