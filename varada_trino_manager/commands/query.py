from ..infra.constants import Paths
from ..infra.configuration import get_config
from ..infra.rest_commands import RestCommands
from ..infra.run_queries import run as query_runner
from ..infra.utils import logger, session_props_to_dict
from click import group, option, Path as ClickPath, argument
from ..infra.query_json_jstack import run as query_json_jstack


@group()
def query():
    """
    Query utility commands
    """
    pass


@option(
    "-f",
    "--filepath",
    type=ClickPath(exists=True),
    help="Location of TXT file with sql to be run, where queries (SQL statements) are separated by ';'",
)
@option(
    "-j",
    "--jsonpath",
    type=ClickPath(exists=True),
    help="""Location of JSON with queries to be run.
    JSON format as per the below example:
    
    \b
    {
        "Query1": "select count(*) from varada.<SCHEMA>.<TABLE>",
        "Query2": "select col40 from varada.<SCHEMA>.<TABLE>",
        "Query3": "select col45, col63 from varada.<SCHEMA>.<TABLE>"
    }
    \b
    
    i.e. dictionary of queries where the keys - "Query1", "Query2"... are the query names, 
    and the values are the corresponding query SQL statements.
    """,
)
@option(
    "-c",
    "--concurrency",
    type=int,
    default=0,
    help="Concurrency factor for parallel queries execution when random option selected.",
)
@option(
    "-r",
    "--random",
    is_flag=True,
    default=False,
    help="Select random query. Concurrency (-c) must be specified as well",
)
@option("-i", "--iterations", type=int, default=1, help="Number of iterations to run")
@option(
    "-s",
    "--sleep",
    type=int,
    default=0,
    help="Number of seconds to sleep between iterations",
)
@option(
    "-g",
    "--get-results",
    is_flag=True,
    default=False,
    help="Print query results. Returns full results for a single query, up to 10 rows for multiple queries. "
         "Please mind the results set size for log readability and test machine mem size.",
)
@option(
    "-p",
    "--session-properties",
    type=str,
    default=None,
    help="Session property(ies) to set prior to running the queries, in the form of: key=value or for multiple: key1=value1,key2=value2... ",
)
@option(
    "-ca",
    "--catalog",
    type=str,
    default='varada',
    help="Catalog to run the queries on, default is varada",
)
@argument("queries_list", nargs=-1)
@query.command()
def runner(
    jsonpath,
    filepath,
    concurrency,
    random,
    iterations,
    sleep,
    queries_list,
    get_results,
    session_properties,
    catalog,
):
    """
    Run queries on Varada Cluster, per the following examples:

    \b
        vtm query runner -j <queries.json> q1                 => Run q1 a single time, where q1 is the key in queries.json
        vtm query runner -f <queries>                  => Run all sql statements from <queries> text file serially, where ';' separates queries in the file
        vtm query runner -j <queries.json> -i 3 q2,q3         => Run q2,q3 serially, iterate 3 times
        vtm -v query runner -j <queries.json> q1,q2,q3 q4,q5     => Run q1,q2,q3 serially, run in parallel q4,q5, be verbose
        vtm -v query runner -f <queries> 0,1,2 3,4      => Same as above, for text file where queries_list consists of indices of sql statements in the file (starting with 0)
        vtm query runner -j <queries.json> -c 6 -r            => Run randomly selected queries to run with concurrency 6
    \b
    """
    con = get_config().get_connection_by_name("coordinator")
    properties = (
        session_props_to_dict(session_properties) if session_properties else None
    )
    query_runner(
        user=con.username,
        jsonpath=jsonpath,
        txtpath=filepath,
        concurrency=concurrency,
        random=random,
        iterations=iterations,
        sleep_time=sleep,
        queries_list=[q_series.split(",") for q_series in queries_list],
        con=con,
        get_results=get_results,
        session_properties=properties,
        catalog=catalog if catalog else 'varada'
    )


@option(
    "-d",
    "--destination-dir",
    type=ClickPath(),
    default=Paths.logs_path,
    help="Destination dir to save the json",
)
@argument("query_id", nargs=1)
@query.command()
def json(query_id, destination_dir):
    """
    Get query json by query_id
    Where query_id is the unique Trino Query Id, format example: 20210513_063641_00004_raiip
    """
    con = get_config().get_connection_by_name("coordinator")
    logger.info(
        f"Getting query json for query_id {query_id}, saving to {destination_dir}/"
    )
    RestCommands.save_query_json(con=con, dest_dir=destination_dir, query_id=query_id)


@option(
    "-d",
    "--destination-dir",
    type=ClickPath(),
    default=Paths.logs_path,
    help="Destination dir to save the files",
)
@option(
    "-j",
    "--jsonpath",
    type=ClickPath(exists=True),
    required=True,
    help='Location of JSON file with query to run: {"query_name":"sql to run"}',
)
@option(
    "-w",
    "--jstack-wait",
    type=int,
    default=0.5,
    help="Number of seconds to wait between jstack collections, default 0.5",
)
@option(
    "-p",
    "--session-properties",
    type=str,
    default=None,
    help="Session property(ies) to set prior to running the queries, in the form of: key=value or for multiple: key1=value1,key2=value2... ",
)
@option(
    "-ca",
    "--catalog",
    type=str,
    default='varada',
    help="Catalog to run the queries on, default is varada",
)
@argument("query_name", nargs=1)
@query.command()
def json_jstack(destination_dir, jsonpath, jstack_wait, query_name, session_properties, catalog):
    """
    Run query and collect jstack from all nodes, collect query json once completed.
    """
    con = get_config().get_connection_by_name("coordinator")
    properties = (
        session_props_to_dict(session_properties) if session_properties else None
    )
    query_json_jstack(
        user=con.username,
        con=con,
        jsonpath=jsonpath,
        query=query_name,
        jstack_wait=jstack_wait,
        dest_dir=destination_dir,
        session_properties=properties,
        catalog=catalog,
    )
