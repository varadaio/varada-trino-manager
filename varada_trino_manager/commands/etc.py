from ..configuration import get_config
from click import group, argument, echo
from ..rest_commands import RestCommands, PrestoRest
from ..remote import parallel_ssh_execute, rest_execute


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
    echo(rest_execute(con=con, rest_client_type=PrestoRest, func=RestCommands.info))


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
