from typing import Tuple
from click import group, argument, echo
from ..infra.remote import ssh_session, parallel_ssh_execute
from ..infra.options import add_options, TARGET_MAP, NODES_OPTIONS


@group()
def ssh():
    """
    SSH related operations
    """
    pass


@argument("node", default="coordinator", nargs=1)
@ssh.command()
def connect(node):
    """
    Start ssh session with one of the nodes
    example: coordinator/node-1/node-2
    default: coordinator
    """
    ssh_session(node=node)



@argument("command", nargs=-1)
@add_options(NODES_OPTIONS)
@ssh.command()
def command(target: str, command: Tuple[str]):
    """
    Send command via SSH to all nodes
    """
    coordinator, workers = TARGET_MAP[target]
    for task, hostname in parallel_ssh_execute(" ".join(command), coordinator=coordinator, workers=workers):
        echo(f"{hostname}: {task.result()}")
