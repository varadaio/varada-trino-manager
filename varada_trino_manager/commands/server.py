from click import group, echo
from ..infra.connections import Trino
from ..infra.configuration import get_config
from ..infra.rest_commands import RestCommands
from ..infra.remote import parallel_ssh_execute, rest_execute


@group()
def server():
    """
    Server management related commands
    """
    pass


@server.command()
def stop():
    """
    Start presto service
    """
    parallel_ssh_execute(command="sudo systemctl stop presto")


@server.command()
def start():
    """
    Start presto service
    """
    parallel_ssh_execute(command="sudo systemctl start presto")


@server.command()
def restart():
    """
    Restart presto service
    """
    parallel_ssh_execute(command="sudo systemctl restart presto")


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