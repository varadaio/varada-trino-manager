from __future__ import annotations
from enum import Enum
from click import exceptions
from typing import List, Union
from .utils import read_file_as_json, logger
from .constants import Paths, InvalidNodeError, Common
from pydantic import BaseModel, StrictStr, StrictInt, error_wrappers


class BrandEnum(str, Enum):
    presto = 'presto'
    trino = 'trino'


class RoleEnum(str, Enum):
    coordinator = 'coordinator'
    worker = 'worker'


class DistributionConfiguration(BaseModel):
    brand: Union[BrandEnum, None]
    port: Union[StrictInt, None]


class VaradaConfiguration(BaseModel):
    port: Union[StrictInt, None]


class Connection(BaseModel):
    hostname: StrictStr
    port: StrictInt
    username: StrictStr
    bastion_hostname: Union[StrictStr, None]
    bastion_port: Union[StrictInt, None]
    bastion_username: Union[StrictStr, None]
    role: RoleEnum
    distribution: DistributionConfiguration
    varada: VaradaConfiguration

    @property
    def with_bastion(self) -> bool:
        return all([self.bastion_hostname, self.bastion_port, self.bastion_username])

    def __repr__(self) -> str:
        bastion_connection_string = (
            f"{self.bastion_username}@{self.bastion_hostname}:{self.bastion_port} --> "
            if bool(
                self.bastion_hostname and self.bastion_port and self.bastion_username
            )
            else ""
        )
        return f"<{self.role}> {bastion_connection_string}{self.username}@{self.hostname}:{self.port}"

    def __str__(self) -> str:
        return repr(self)

class BastionConfiguration(BaseModel):
    hostname: Union[StrictStr, None]
    port: Union[StrictInt, None]
    username: Union[StrictStr, None]


class Configuration(BaseModel):
    coordinator: StrictStr
    workers: List[StrictStr]
    username: StrictStr
    port: StrictInt
    bastion: BastionConfiguration
    distribution: DistributionConfiguration
    varada: VaradaConfiguration

    @property
    def is_single(self) -> bool:
        return len(self.workers) == 1 and self.coordinator == self.workers[0]

    @classmethod
    def from_json(cls, file_path: str) -> Configuration:
        data = read_file_as_json(file_path=file_path)
        bastion_data = data.get("bastion", dict())
        distribution_data = data.get('distribution', dict())
        varada_data = data.get('varada', dict())
        try:
            bastion = BastionConfiguration(
                hostname=bastion_data.get("hostname"),
                port=bastion_data.get("port"),
                username=bastion_data.get("username"),
            )
            distribution = DistributionConfiguration(
                brand=distribution_data.get('brand', BrandEnum.trino),
                port=distribution_data.get('port', Common.API_PORT)
            )
            varada = VaradaConfiguration(port=varada_data.get('port', Common.VARADA_PORT))
            return cls(
                coordinator=data.get("coordinator"),
                workers=data.get("workers"),
                username=data.get("username"),
                port=data.get("port"),
                bastion=bastion,
                distribution=distribution,
                varada=varada
            )
        except error_wrappers.ValidationError as e:
            logger.error(f"Configuration is malformed: {e}")
            raise exceptions.Exit(code=1)

    @property
    def number_of_nodes(self) -> int:
        if self.is_single:
            return 1
        return len(self.workers) + 1

    def iter_workers_connections(self):
        for node in self.workers:
            yield Connection(
                hostname=node,
                port=self.port,
                username=self.username,
                bastion_port=self.bastion.port,
                bastion_hostname=self.bastion.hostname,
                bastion_username=self.bastion.username,
                role=RoleEnum.worker,
                distribution=self.distribution,
                varada=self.varada
            )

    @property
    def coordinator_connection(self):
        return Connection(
            hostname=self.coordinator,
            port=self.port,
            username=self.username,
            bastion_port=self.bastion.port,
            bastion_hostname=self.bastion.hostname,
            bastion_username=self.bastion.username,
            role=RoleEnum.coordinator,
            distribution=self.distribution,
            varada=self.varada
        )

    def iter_connections(self) -> Connection:
        yield self.coordinator_connection
        if not self.is_single:
            yield from self.iter_workers_connections()

    def get_connection_by_name(self, node: str) -> Connection:
        if node == "coordinator":
            role = RoleEnum.coordinator
            hostname = self.coordinator
        elif node.startswith("node-"):
            role = RoleEnum.worker
            _, position = node.split("-")
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
            distribution=self.distribution,
            varada=self.varada
        )


def get_config() -> Configuration:
    return Configuration.from_json(Paths.config_path)
