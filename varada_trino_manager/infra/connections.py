from os import makedirs
from .utils import logger
from getpass import getuser
from typing import Tuple, Union
from dataclasses import dataclass
from os.path import exists, dirname
from abc import ABCMeta, abstractmethod
from sshtunnel import SSHTunnelForwarder
from paramiko.transport import Transport
from paramiko.sftp_client import SFTPClient
from paramiko import AutoAddPolicy, SSHClient
from .configuration import Connection, BrandEnum
from trino.dbapi import Connection as TrinoConnection
from requests import Session, Response, codes, exceptions
from prestodb.dbapi import Connection as PrestoConnection


class Client(metaclass=ABCMeta):
    def __init__(self, con: Connection, port: int):
        self.__con = con
        self.__port = port
        self.__host = con.hostname
        self.__tuneel = None

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    def __enter__(self):
        if self.__con.with_bastion:
            self.__tuneel = SSHTunnelForwarder(
                ssh_address_or_host=(
                    self.__con.bastion_hostname,
                    self.__con.bastion_port,
                ),
                ssh_username=self.__con.bastion_username,
                remote_bind_address=(self.__con.hostname, self.port),
                allow_agent=True,
            )
            self.__tuneel.start()
            self.__host, self.__port = self.__tuneel.local_bind_address
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if self.__con.with_bastion:
            self.__tuneel.stop()

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @property
    def connection(self) -> Connection:
        return self.__con

class SSH(Client):
    PORT = 22
    LOCALHOST: str = "127.0.0.1"

    def __init__(self, con: Connection):
        super(SSH, self).__init__(con=con, port=con.port)
        self.__user = con.username

    def connect(self):
        self.__client = SSHClient()
        self.__client.set_missing_host_key_policy(AutoAddPolicy)
        self.__client.connect(
            hostname=self.host,
            port=self.port,
            username=self.__user,
            allow_agent=True,
        )

    def close(self):
        self.__client.close()

    def get_transport(self) -> Transport:
        return self.__client.get_transport()

    def execute(self, command: str) -> str:
        logger.debug(f"<{self.host}>Executing: {command}")
        _, stdout, _ = self.__client.exec_command(command=command)
        return stdout.read().decode()


class SFTP(SSH):
    def connect(self):
        super(SFTP, self).connect()
        self.__client = SFTPClient.from_transport(self.get_transport())

    def get(self, remote_file_path: str, local_file_path: str):
        if not exists(dirname(local_file_path)):
            makedirs(dirname(local_file_path))
        self.__client.get(remotepath=remote_file_path, localpath=local_file_path)

    def put(self, local_file_path: str, remote_file_path: str):
        self.__client.put(localpath=local_file_path, remotepath=remote_file_path)


@dataclass
class Schemas:
    HTTP: str = "http"
    HTTPS: str = "https"


def handle_response(func):
    def handle_response_wrapper(self, *args, **kw) -> Response:
        response = func(self, *args, **kw)
        if response.status_code == codes.ok:
            return response
        raise exceptions.HTTPError(response=response)

    return handle_response_wrapper


class Rest(Client):
    def __init__(self, con: Connection, http_schema: str = Schemas.HTTP, port: int = None):
        super(Rest, self).__init__(con=con, port=port)
        self.__http_schema = http_schema

    def connect(self):
        self.__client = Session()

    def close(self) -> None:
        if self.__client is not None:
            del self.__client

    @property
    def url(self) -> str:
        return f"{self.__http_schema}://{self.host}:{self.port}"

    @handle_response
    def get(self, sub_url: str, headers: dict = None) -> Response:
        url = f"{self.url}/{sub_url}"
        logger.debug(f"GET {url}")
        return self.__client.get(url=url, headers=headers)

    @handle_response
    def post(self, sub_url: str, json_data: Union[dict, list] = None, headers: dict = None) -> Response:
        url = f"{self.url}/{sub_url}"
        logger.debug(f"POST {url} {json_data}")
        return self.__client.post(url=url, json=json_data, headers=headers)


class ExtendedRest(Rest):
    

    brand_to_header_key = {
        BrandEnum.presto: 'X-Presto-User',
        BrandEnum.trino: 'X-Trino-User'
    }

    def __init__(self, con: Connection, http_schema, port: int = None):
        super().__init__(con, http_schema=http_schema, port=port or con.distribution.port)
        self.__brand = con.distribution.brand

    @property
    def headers(self) -> dict:
        header_key = self.brand_to_header_key.get(self.__brand)
        if header_key is None:
            raise ValueError(f'Invalid brand: {self.__brand}')
        return {header_key: 'varada'}

    @property
    def url(self) -> str:
        return f"{super(ExtendedRest, self).url}/v1"

    def query_json(self, query_id: str):
        return self.get(sub_url=f'query/{query_id}?pretty', headers=self.headers)


class VaradaRest(Rest):
    
    PORT = 8088

    def __init__(self, con: Connection, http_schema: str):
        super().__init__(con, http_schema=http_schema, port=self.PORT)

    @property
    def url(self) -> str:
        return f"{super(VaradaRest, self).url}/v1/ext/varada"

    def row_group_count(self):
        return self.post(sub_url='row-group-count', json_data={"commandName": "all"})

    def get_warmup_rules(self):
        rules = self.post(sub_url='warmup-rule-get')
        return rules.json()

    def set_warmup_rule(self, json_data: dict):
        json_data['predicates'] = [] if not json_data['predicates'] else json_data['predicates']
        return self.post(sub_url='warmup-rule-set', json_data=[json_data])

    def del_warmup_rule(self, json_data: int):
        return self.post(sub_url='warmup-rule-delete', json_data=[json_data])

    def log(self, msg: str) -> None:
        self.post(sub_url='debug-log', json_data={'logLine': msg})


class APIClient(Client):

    distribution_to_class = {
        BrandEnum.trino: TrinoConnection,
        BrandEnum.presto: PrestoConnection
    }

    def __init__(
        self, con: Connection, username: str = None, http_schema: str = Schemas.HTTP, session_properties: dict = None,
    ):
        super(APIClient, self).__init__(con=con, port=con.distribution.port)
        self.__username = getuser() if username is None else username
        self.__http_schema = http_schema
        self.__session_properties = session_properties

    def connect(self):
        cls = self.distribution_to_class.get(self.connection.distribution.brand)
        if cls is None:
            raise ValueError(f'Invalid distribution: {self.connection.distribution.brand}')
        self.__client = cls(
            host=self.host,
            port=self.port,
            user=self.__username,
            http_scheme=self.__http_schema,
            http_headers={},
            session_properties=self.__session_properties,
        )

    def close(self):
        del self.__client

    def execute(self, query: str, fetch_all: bool = True) -> Tuple[list, dict]:
        try:
            logger.debug(f"Executing: {query.encode()}")
            with self.__client as con:
                cursor = con.cursor()
                cursor.execute(query)
                result = cursor.fetchall() if fetch_all else cursor.fetchone()
            return result, cursor.stats
        except Exception as e:
            logger.exception(f"Failed to execute query: {query}")
            raise e

    def set_session(self, key: str, value: str) -> None:
        self.execute(f"SET SESSION {key}={value}")

    def reset_session(self, key: str) -> None:
        self.execute(f"RESET SESSION {key}")