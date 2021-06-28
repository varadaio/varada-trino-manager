from ..infra.utils import logger
from ..infra.configuration import get_config
from ..infra.warm_validate import run as warm_validate
from click import group, option, Path as ClickPath, exceptions
from ..infra.rules import apply as apply_rule, get as get_rule, delete as delete_rule


@group()
def rules():
    """
    Rules utility commands
    """


@option(
    "-c",
    "--csv-path",
    type=ClickPath(exists=True),
    help="""Full path and name of CSV file with rule(s) to set.
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
""",
)
@option(
    "-j",
    "--json-path",
    type=ClickPath(exists=True),
    help="""Location of JSON with rule(s) to set.
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
""",
)
@rules.command()
def apply(json_path, csv_path):
    """
    Apply rules to the cluster from json or csv file
    """
    con = get_config().get_connection_by_name("coordinator")
    apply_rule(con=con, json_path=json_path, csv_path=csv_path)


@option(
    "-t",
    "--table",
    type=str,
    required=False,
    help="Get user rules associated with table",
)
@option(
    "-c",
    "--column",
    type=str,
    required=False,
    help="Get user rules associated with column. Must specify table as well",
)
@option(
    "-d",
    "--destination-dir",
    type=ClickPath(),
    help="Save rules to destination dir as json file",
)
@rules.command()
def get(table, column, destination_dir):
    """
    Get rules from the Varada cluster, by default retrieve all
    """
    con = get_config().get_connection_by_name("coordinator")
    get_rule(con=con, table=table, column=column, destination_dir=destination_dir)


@option(
    "-a",
    "--all-rules",
    is_flag=True,
    default=False,
    help="Delete all user rules from the Varada cluster. It is recommended to backup the rules first by running: vtm rules get -a",
)
@option(
    "-i",
    "--rule-ids",
    type=str,
    default=None,
    help="ID of rule to be deleted, if multiple - comma separated. Example: vtm -v rules delete -i 1106600307,1830309151",
)
@rules.command()
def delete(rule_ids, all_rules):
    """
    Delete rule from the cluster
    """
    con = get_config().get_connection_by_name("coordinator")
    if (rule_ids is None) and not all_rules:
        logger.info("Either -a or -i option is required")
        raise exceptions.Exit(code=1)
    proceed = input(
        f'Delete rules: {rule_ids if rule_ids else "all rules"} from the cluster? [y/N]: '
    )
    if proceed == "n":
        logger.info("Aborting, no rules will be deleted")
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
