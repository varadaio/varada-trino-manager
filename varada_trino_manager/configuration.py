from __future__ import annotations
from .constants import Paths, InvalidNodeError
from dataclasses import dataclass
from .utils import read_file_as_json
from typing import List, Union


@dataclass
class Connection:
    hostname: str
    port: str
    username: str
    bastion_hostname: Union[str, None]
    bastion_port: Union[int, None]
    bastion_username: Union[str, None]
    role: str

    @property
    def with_bastion(self) -> bool:
        return all([self.bastion_hostname, self.bastion_port, self.bastion_username])

    def __repr__(self) -> str:
        bastion_connection_string = (
            f"{self.bastion_username}@{self.bastion_hostname}:{self.bastion_port}->"
            if bool(
                self.bastion_hostname and self.bastion_port and self.bastion_username
            )
            else ""
        )
        return f"<{self.role}>{bastion_connection_string}{self.username}@{self.hostname}:{self.port}"


@dataclass
class BastionConfiguration:
    hostname: Union[str, None]
    port: Union[int, None]
    username: Union[str, None]


@dataclass
class Configuration:
    coordinator: str
    workers: List[str]
    username: str
    port: int
    bastion: BastionConfiguration

    @classmethod
    def from_json(cls, file_path: str) -> Configuration:
        data = read_file_as_json(file_path=file_path)
        bastion_data = data.get("bastion", dict())
        bastion = BastionConfiguration(
            hostname=bastion_data.get("hostname"),
            port=bastion_data.get("port"),
            username=bastion_data.get("username"),
        )
        return cls(
            coordinator=data.get("coordinator"),
            workers=data.get("workers"),
            username=data.get("username"),
            port=data.get("port"),
            bastion=bastion,
        )

    @property
    def number_of_nodes(self) -> int:
        return len(self.workers) + 1

    def iter_connections(
        self,
    ) -> Connection:
        role = "coordinator"
        for node in [self.coordinator] + self.workers:
            yield Connection(
                hostname=node,
                port=self.port,
                username=self.username,
                bastion_port=self.bastion.port,
                bastion_hostname=self.bastion.hostname,
                bastion_username=self.bastion.username,
                role=role,
            )
            role = "worker"

    def get_connection_by_name(self, node: str) -> Connection:
        if node == "coordinator":
            role = node
            hostname = self.coordinator
        elif node.startswith("node-"):
            role, position = node.split("-")
            if position.isdigit():
                position = int(position)
            else:
                raise InvalidNodeError(f"Got invalid node: {node}")
            if position > len(self.workers):
                raise InvalidNodeError(
                    f"Worker node out of range, got {node}, but there are only {len(self.workers)} workers"
                )
            hostname = self.workers[position]
        else:
            raise InvalidNodeError(f"Got invalid node: {node}")
        return Connection(
            hostname=hostname,
            port=self.port,
            username=self.username,
            bastion_port=self.bastion.port,
            bastion_hostname=self.bastion.hostname,
            bastion_username=self.bastion.username,
            role=role,
        )


def get_config() -> Configuration:
    return Configuration.from_json(Paths.config_path)
