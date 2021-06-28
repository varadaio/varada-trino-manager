from json import dumps
from ..infra.constants import Paths
from click import group, echo
from ..infra.utils import read_file_as_json


@group()
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
