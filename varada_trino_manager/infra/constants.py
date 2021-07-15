from os import environ
from pathlib import Path
from os.path import expanduser
from dataclasses import dataclass


class InvalidNodeError(Exception):
    pass


@dataclass
class Paths:
    config_dir: Path = Path(environ.get("VARADA_TRINO_MANAGER_DIR", expanduser("~/.vtm")))
    config_file_name: str = "config.json"
    config_path: Path = config_dir / config_file_name
    logs_path: Path = config_dir / "logs"


class Common:
    SSH_ARGS = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o TCPKeepAlive=yes -o ServerAliveInterval=150 -o ServerAliveCountMax=4"
    SSH_ARGS_AGENT_FORWARDING = f"{SSH_ARGS} -A -tt"
    API_PORT = 8080
