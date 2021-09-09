from ..infra.constants import Paths
from ..infra.connections import VaradaRest
from ..infra.rest_commands import RestCommands
from click import group, option, Path as ClickPath, argument
from ..infra.remote import parallel_ssh_execute, parallel_download, parallel_rest_execute


@group()
def logs():
    """
    Logs related commands
    """
    pass


@argument("msg", nargs=-1)
@logs.command()
def send(msg):
    """
    Write to nodes logs
    """
    # concat tuple arg into one string
    send_msg = " ".join(msg)
    parallel_rest_execute(rest_client_type=VaradaRest, func=RestCommands.dev_log, msg=send_msg)


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
