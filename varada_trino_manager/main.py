from json import dumps
from typing import Tuple
from logbook import WARNING
from .warm_validate import run as warm_validate
from .constants import Paths
from .configuration import get_config
from .rest_commands import RestCommands
from .connections import PrestoRest, Trino
from .utils import read_file_as_json, logger
from .remote import parallel_download, parallel_ssh_execute, rest_execute, ssh_session
from click import group, argument, option, echo, Path as ClickPath, exceptions


@option("-v", "--verbose", count=True, help="Be more verbose")
@group()
def main(verbose):
    """
    Varada trino manager
    """
    if verbose > 5:
        logger.error('Can get up to 5 "-v"')
        raise exceptions.Exit(code=1)
    logger.level = (
        WARNING if verbose == 0 else 10 + verbose
    )  # logbook levels run from 10 to 15


@main.group()
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
@ssh.command()
def command(command: Tuple[str]):
    """
    Send command via SSH to all nodes
    """
    for task, hostname in parallel_ssh_execute(" ".join(command)):
        echo(f"{hostname}: {task.result()}")


@main.group()
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
def is_panic():
    """
    Verify if a node is in panic
    """
    command = "tail -n 30 /var/log/presto/launcher.log | grep -i panic | wc -l"
    tasks = parallel_ssh_execute(command=command)
    for panic, hostname in tasks:
        if bool(int(panic.result().strip())):
            echo(f"found panic in {hostname}")


@main.group()
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


@main.group()
def logs():
    """
    Logs related commands
    """
    pass


@logs.command()
def clear():
    """
    Clear logs
    """
    parallel_ssh_execute(command="rm /var/log/presto/*")


@option(
    "-d",
    "--destination-dir",
    type=ClickPath(),
    default=None,
    help="Destination dir to save the logs",
)
@logs.command()
def collect(destination_dir: str):
    """
    Collect fresh logs and store in logs dir, overwiting existing one
    """
    commands = [
        "sudo rm -rf /tmp/custom_logs",
        "mkdir /tmp/custom_logs",
        "sudo dmesg > /tmp/custom_logs/dmesg",
        "sudo jps > /tmp/custom_logs/jps",
        'grep TrinoServer /tmp/custom_logs/jps | cut -d" " -f1 > /tmp/custom_logs/server.pid || true',
        "sudo jstack $(cat /tmp/custom_logs/server.pid) > /tmp/custom_logs/jstack.txt || true",
        "sudo pstack $(cat /tmp/custom_logs/server.pid) > /tmp/custom_logs/pstack.txt || true",
        "cp /var/log/presto/* /tmp/custom_logs/ || true",
        "sudo cp /var/log/messages /tmp/custom_logs/",
        "sudo cp /var/log/user-data.log /tmp/custom_logs/",
        "sudo tar -C /tmp/custom_logs -zcf /tmp/custom_logs.tar.gz .",
        "sudo chmod 777 /tmp/custom_logs.tar.gz",
    ]
    parallel_ssh_execute(command="\n".join(commands))
    dir_path = Paths.logs_path if destination_dir is None else destination_dir
    parallel_download(
        remote_file_path="/tmp/custom_logs.tar.gz", local_dir_path=dir_path
    )


@main.group()
def rules():
    """
    Rules utility commands
    """


@rules.command()
def generate():
    """
    Generate rule
    """
    pass


@rules.command()
def apply():
    """
    Apply rule to the cluster
    """
    pass


@rules.command()
def get():
    """
    Get rule from the cluster
    """
    pass


@rules.command()
def delete():
    """
    Delete rule from the cluster
    """
    pass


@option('-p', '--presto-host', required=True, default="http://localhost:8080",
        help='Varada coordinator url. For example: http://1.2.3.4:8080')
@option("-u", "--user", type=str, default='benchmarker', help='user for coordinator, default=benchmarker')
@option("-j", "--jsonpath", type=ClickPath(exists=True), required=True, help="""Location of JSON with list of queries.
JSON format as per the below example:
\b
{
"warm_queries": [
  "select count(<col1>), count(<col2>),... count(<colN>) from varada.<SCHEMA>.<TABLE>"
]
  }
\b
i.e. list of warm_queries where col1, col2,... colN are columns which have warmup rules applied
""")
@rules.command()
def warm_and_validate(presto_host, user, jsonpath):
    """
    Warmup Varada per rules applied
    """
    warm_validate(presto_host=presto_host, user=user, jsonpath=jsonpath)


@main.group()
def config():
    """
    Config related commands
    """
    pass


@config.command()
def show():
    """
    Show current configuration
    """
    data = read_file_as_json(Paths.config_path)
    echo(dumps(data, indent=2))


@config.command()
def template():
    """
    Show configuration template
    """
    data = {
        "coordinator": "coordinator.example.com",
        "workers": [
            "worker1.example.com",
            "worker2.example.com",
            "worker3.example.com",
        ],
        "port": 22,
        "username": "root",
    }
    echo(f"Simple:\n{dumps(data, indent=2)}")
    echo("")  # new line
    data["bastion"] = {
        "hostname": "bastion.example.com",
        "port": 22,
        "username": "root",
    }
    echo(f"With bastion:\n{dumps(data, indent=2)}")


if __name__ == "__main__":
    main()
