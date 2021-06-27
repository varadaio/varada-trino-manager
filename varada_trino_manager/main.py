from json import dumps
from typing import Tuple
from .constants import Paths
from json import dumps
from .configuration import get_config
from .rest_commands import RestCommands
from .connections import PrestoRest, Trino
from .run_queries import run as query_runner
from logging import INFO, DEBUG, StreamHandler
from .warm_validate import run as warm_validate
from .query_json_jstack import run as query_json_jstack
from .rules import delete as delete_rule, get as get_rule, apply as apply_rule
from click import group, argument, option, echo, Path as ClickPath, exceptions
from .utils import read_file_as_json, logger, session_props_to_dict
from .remote import (
    parallel_download,
    parallel_ssh_execute,
    rest_execute,
    ssh_session,
    parallel_upload,
)


@option("-v", "--verbose", is_flag=True, default=False, help="Be more verbose")
@group()
def main(verbose):
    """
    Varada trino manager
    """
    for handler in logger.handlers:
        if type(handler) == StreamHandler:
            handler.setLevel(DEBUG if verbose else INFO)


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


@option("-c", "--csv-path", type=ClickPath(exists=True), help="""Full path and name of CSV file with rule(s) to set.
CSV format as per the below example:

\b
# Headers row, then row per rule:
schema,table,colNameId,colWarmUpType,priority,ttl,predicates
default,trips_partitions_table,tripid,COL_WARM_UP_TYPE_BASIC,8,PT720H,"type:PartitionValue,columnId:d_date,value:2018-01-02","type:PartitionValue,columnId:d_date,value:2018-01-03"
default,trips_date_int_date_table,char_10,COL_WARM_UP_TYPE_DATA,7,PT50M,"type:DateRangeSlidingWindow,columnId:date_date,windowDateFormat:yyyy-MM-dd,startRangeDaysBefore:450,endRangeDaysBefore:448"
...
\b
Note that predicate sets are double quoted
For WarmUpType/predicate examples and more info per CSV column see https://docs.varada.io/docs/acceleration-instruction-commands
""")
@option("-j", "--json-path", type=ClickPath(exists=True), help="""Location of JSON with rule(s) to set.
JSON format as per the below basic example:

\b
{
    "schema": "SCHEMA",
    "table": "TABLE",
    "colNameId": "COLUMN_NAME",
    "colWarmUpType": "COL_WARM_UP_TYPE_BASIC",
    "priority": 8,
    "ttl": "PT0M",
    "predicates": []
}
\b

For predicate examples and more info per key see https://docs.varada.io/docs/acceleration-instruction-commands
""")
@rules.command()
def apply(json_path, csv_path):
    """
    Apply rules to the cluster from json or csv file
    """
    con = get_config().get_connection_by_name("coordinator")
    apply_rule(con=con, json_path=json_path, csv_path=csv_path)


@option("-t", "--table", type=str, required=False, help='Get user rules associated with table')
@option("-c", "--column", type=str, required=False, help='Get user rules associated with column. Must specify table as well')
@option("-d", "--destination-dir", type=ClickPath(), help="Save rules to destination dir as json file")
@rules.command()
def get(table, column, destination_dir):
    """
    Get rules from the Varada cluster, by default retrieve all
    """
    con = get_config().get_connection_by_name("coordinator")
    get_rule(con=con, table=table, column=column, destination_dir=destination_dir)


@option("-a", "--all-rules", is_flag=True, default=False, help="Delete all user rules from the Varada cluster. It is recommended to backup the rules first by running: vtm rules get -a")
@option("-i", "--rule-ids", type=str, default=None, help='ID of rule to be deleted, if multiple - comma separated. Example: vtm -v rules delete -i 1106600307,1830309151')
@rules.command()
def delete(rule_ids, all_rules):
    """
    Delete rule from the cluster
    """
    con = get_config().get_connection_by_name("coordinator")
    if (rule_ids is None) and not all_rules:
        logger.info('Either -a or -i option is required')
        raise exceptions.Exit(code=1)
    proceed = input(f'Delete rules: {rule_ids if rule_ids else "all rules"} from the cluster? [y/N]: ')
    if proceed == 'n':
        logger.info('Aborting, no rules will be deleted')
        raise exceptions.Exit(code=1)
    delete_rule(con=con, rule_ids=rule_ids, all_rules=all_rules)


@option(
    "-u",
    "--user",
    type=str,
    default="benchmarker",
    help="user for coordinator, default=benchmarker",
)
@option(
    "-j",
    "--jsonpath",
    type=ClickPath(exists=True),
    required=True,
    help="""Location of JSON with list of queries.
JSON format as per the below example:
\b
{
"warm_queries": [
  "select count(<col1>), count(<col2>),... count(<colN>) from varada.<SCHEMA>.<TABLE>"
]
  }
\b
i.e. list of warm_queries where col1, col2,... colN are columns which have warmup rules applied
""",
)
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
@option("-s", "--sleep", type=int, default=0, help="Number of seconds to sleep between iterations")
@option("-g", "--get-results", is_flag=True, default=False, help="Print query results. Please mind the results set size for log readability and test machine mem size")
@option("-p", "--session-properties", type=str, default=None, help="Session property(ies) to set prior to running the queries, in the form of: key=value or for multiple: key1=value1,key2=value2... ")
@argument("queries_list", nargs=-1)
@query.command()
def runner(jsonpath, concurrency, random, iterations, sleep, queries_list, get_results, session_properties):
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
    properties = session_props_to_dict(session_properties) if session_properties else None

    query_runner(user=con.username, jsonpath=jsonpath, concurrency=concurrency, random=random, iterations=iterations,
                 sleep_time=sleep, queries_list=[q_series.split(',') for q_series in queries_list], con=con,
                 get_results=get_results, session_properties=properties)


@option("-d", "--destination-dir", type=ClickPath(), default=Paths.logs_path, help="Destination dir to save the json")
@argument("query_id", nargs=1)
@query.command()
def json(query_id, destination_dir):
    """
    Get query json by query_id
    Where query_id is the unique Trino Query Id, format example: 20210513_063641_00004_raiip
    """
    con = get_config().get_connection_by_name("coordinator")
    logger.info(f'Getting query json for query_id {query_id}, saving to {destination_dir}/')
    RestCommands.save_query_json(con=con, dest_dir=destination_dir, query_id=query_id)


@option("-d", "--destination-dir", type=ClickPath(), default=Paths.logs_path, help="Destination dir to save the files")
@option("-j", "--jsonpath", type=ClickPath(exists=True), required=True, help="Location of JSON file with query to run: {\"query_name\":\"sql to run\"}")
@option("-w", "--jstack-wait", type=int, default=0.5, help="Number of seconds to wait between jstack collections, default 0.5")
@option("-p", "--session-properties", type=str, default=None, help="Session property(ies) to set prior to running the queries, in the form of: key=value or for multiple: key1=value1,key2=value2... ")
@argument("query_name", nargs=1)
@query.command()
def json_jstack(destination_dir, jsonpath, jstack_wait, query_name, session_properties):
    """
    Run query and collect jstack from all nodes, collect query json once completed.
    """
    con = get_config().get_connection_by_name("coordinator")
    properties = session_props_to_dict(session_properties) if session_properties else None
    query_json_jstack(user=con.username, con=con, jsonpath=jsonpath, query=query_name, jstack_wait=jstack_wait,
                      dest_dir=destination_dir, session_properties=properties)


@main.group()
def connector():
    """
    Connector related commands
    """


@option(
    "-t",
    "--targz-path",
    type=ClickPath(),
    help="Path to targz file contains varada connector",
    required=True,
)
@option(
    "-p",
    "--script-params",
    type=str,
    help="Params to pass to the connector install script",
    default=None,
)
@option(
    "-e",
    "--external-install-script-path",
    type=ClickPath(),
    help="External install script path",
    default=None,
)
@option(
    "-i",
    "--installation-dir",
    type=str,
    help="Remote installation directory, for example - /usr/lib/presto",
    default=None,
)
@option(
    "-u",
    "--user",
    type=str,
    help="User that runs the presto server",
    default=None,
)
@connector.command()
def install(targz_path: str, script_params: str, external_install_script_path: str, installation_dir: str, user: str):
    """
    Install Varada connector
    """
    parallel_upload(
        local_file_path=targz_path, remote_file_path="/tmp/varada-connector.tar.gz"
    )
    if external_install_script_path:
        parallel_upload(
            local_file_path=external_install_script_path,
            remote_file_path="/tmp/external-install.py",
        )
    commands = [
        f"sudo usermod -a -G disk {user if user else '$(whoami)'}",
        "mkdir /tmp/varada-install",
        "sudo mkdir -p /var/lib/presto/workerDB",
        "sudo chmod 777 -R /var/lib/presto/workerDB",
        "tar -zxf /tmp/varada-connector.tar.gz -C /tmp/varada-install",
        f"sudo chmod 777 -R {installation_dir}",
        f"cp -R /tmp/varada-install/varada-connector-350/presto/plugin/varada {installation_dir}/plugin/.",
        "sudo cp -R /tmp/varada-install/varada-connector-350/trc /usr/local/trc",
        "sudo ln -sfn /usr/local/trc/trc_decoder /usr/local/bin/trc_decoder",
        "cd /tmp/varada-install/varada-connector-350",
        f"sudo python3 {'/tmp/external-install.py' if external_install_script_path else '/tmp/varada-install/varada-connector-350/varada/installer.py'} {script_params}",
    ]
    for task, hostname in parallel_ssh_execute("\n".join(commands)):
        echo(f"{hostname}: {task.result()}")


if __name__ == "__  main__":
    main()
