from json import dumps
from click import group, echo
from ..infra.constants import Paths
from ..infra.constants import Common
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
    data.update(
        {
            "bastion": {
                "hostname": "bastion.example.com",
                "port": 22,
                "username": "root",
            },
            "distribution": {"brand": "trino", "port": Common.API_PORT},
            "varada": {"port": Common.VARADA_PORT}
        }
    )
    echo(f"With bastion and distribution:\n{dumps(data, indent=2)}")
