from json import dumps
from typing import Tuple
from .constants import Paths
from click import group, argument
from .utils import read_file_as_json
from .remote import parallel_download, parallel_ssh_execute, ssh_session


@group()
def main():
    """
    Varada trino manager
    """
    pass


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
    Start ssh session with one of the nodes, example: coordinator/node-1,node-2
    """
    ssh_session(node=node)


@argument("command", nargs=-1)
@ssh.command()
def command(command: Tuple[str]):
    """
    Send command via SSH to all nodes
    """
    for task, hostname in parallel_ssh_execute(" ".join(command)):
        print(f"{hostname}: {task.result()}")


@main.group()
def etc():
    """
    More utilities
    """
    pass


@etc.command()
def show_deployment():
    """
    Shows the current configuration
    """
    data = read_file_as_json(Paths.config_path)
    print(dumps(data, indent=2))


@etc.command()
def is_panic():
    """
    Verify if a node is in panic
    """
    command = "tail -n 30 /var/log/presto/launcher.log | grep -i panic | wc -l"
    tasks = parallel_ssh_execute(command=command)
    for panic, hostname in tasks:
        if bool(int(panic.result().strip())):
            print(f"found panic in {hostname}")


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


@logs.command()
def collect():
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
    parallel_download(
        remote_file_path="/tmp/custom_logs.tar.gz", local_dir_path=Paths.logs_path
    )


if __name__ == "__main__":
    main()
