from .ssh import ssh
from .etc import etc
from .logs import logs
from .rules import rules
from .query import query
from .server import server
from .config import config
from .connector import connector
from .call_home import call_home

commands_groups = [ssh, etc, logs, rules, query, server, config, connector, call_home]
