from json import dumps
from typing import Tuple
from logbook import WARNING
from .constants import Paths
from .configuration import get_config
from .rest_commands import RestCommands
from .connections import PrestoRest, Trino
from .warm_validate import run as warm_validate
from .run_queries import run as query_runner
from .utils import read_file_as_json, logger, LOG_LEVELS
from click import group, argument, option, echo, Path as ClickPath, exceptions
from .remote import parallel_download, parallel_ssh_execute, rest_execute, ssh_session


@option("-v", "--verbose", count=True, help="Be more verbose")
@group()
def main(verbose):
    """
    Varada trino manager
    """
    if verbose > 4:
        logger.error('Can get up to 4 "-v"')
        raise exceptions.Exit(code=1)
    logger.level = WARNING if verbose == 0 else LOG_LEVELS[verbose]


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
def warm_and_validate(user, jsonpath):
    """
    Warmup Varada per rules applied
    """
    con = get_config().get_connection_by_name("coordinator")
    warm_validate(user=user, jsonpath=jsonpath, con=con)


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


@main.group()
def query():
    """
    Query utility commands
    """
    pass


@option("-j", "--jsonpath", type=ClickPath(exists=True), required=True, help="""Location of JSON with queries to be run.
JSON format as per the below example:

\b
{
    "Query1": "select count(*) from varada.<SCHEMA>.<TABLE>",
    "Query2": "select col40 from varada.<SCHEMA>.<TABLE>",
    "Query3": "select col45, col63 from varada.<SCHEMA>.<TABLE>"
}
\b

i.e. dictionary of queries where the keys - "Query1", "Query2"... are the query names, and the values are the corresponding query SQL statements.
""")
@option("-c", "--concurrency", type=int, default=1, help='Concurrency factor for parallel queries execution')
@option("-r", "--random", is_flag=True, default=False, help="Select random query. If specified will ignore query_list")
@option("-i", "--iterations", type=int, default=1, help="Number of iterations to run")
@argument("queries_list", nargs=-1)
@query.command()
def runner(jsonpath, concurrency, random, iterations, queries_list):
    """
    Run queries on Varada Cluster, per the following examples:

    \b
        vtm -vvvv query runner -j <queries.json> q1                 => Run q1 a single time, where q1 is the key in queries.json
        vtm -vvvv query runner -j <queries.json> -i 3 q2,q3         => Run q2,q3 serially, iterate 3 times
        vtm -vvvv query runner -j <queries.json> q1,q2,q3 q4,q5     => Run q1,q2,q3 serially, run in parallel q4,q5
        vtm -vvvv query runner -j <queries.json> -c 6 -r            => Run randomly selected queries to run with concurrency 6
    \b
    """
    con = get_config().get_connection_by_name("coordinator")
    query_runner(user=con.username, jsonpath=jsonpath, concurrency=concurrency, random=random, iterations=iterations,
                 queries_list=[q_series.split(',') for q_series in queries_list], con=con)


if __name__ == "__main__":
    main()
