from os import makedirs
from .utils import logger
from dataclasses import dataclass
from os.path import exists, dirname
from paramiko.channel import Channel
from paramiko.transport import Transport
from paramiko.sftp_client import SFTPClient
from paramiko import AutoAddPolicy, SSHClient
from requests import Session, Response, codes, exceptions


class Client:
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SSH(Client):
    LOCALHOST: str = "127.0.0.1"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        sock: Channel = None
    ):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__sock = sock
        self.__client = SSHClient()
        self.__transport = None

    def connect(self):
        self.__client.set_missing_host_key_policy(AutoAddPolicy)
        self.__client.connect(
            hostname=self.__host,
            port=self.__port,
            username=self.__user,
            sock=self.__sock,
            allow_agent=True,
        )

    def close(self):
        self.__client.close()

    def get_transport(self) -> Transport:
        return self.__client.get_transport()

    def get_channel(self, target_host: str, target_port: int) -> Channel:
        destination_address = (target_host, target_port)
        source_address = (self.LOCALHOST, self.__port)
        self.__transport = self.__client.get_transport()
        return self.__transport.open_channel(
            "direct-tcpip", dest_addr=destination_address, src_addr=source_address
        )

    def execute(self, command: str) -> str:
        logger.debug(f'executing: {command}')
        _, stdout, _ = self.__client.exec_command(command=command)
        return stdout.read().decode()


class SFTP(SSH):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        sock: Channel = None
    ):
        super(SFTP, self).__init__(
            host=host, port=port, user=user, sock=sock, logger=logger
        )

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
    def __init__(self, host, port: int = None, http_schema: str = Schemas.HTTP):
        self.__host = host
        self.__port = port if port is not None else self.PORT
        self.__http_schema = http_schema

    def connect(self):
        self.__client = Session()

    def close(self) -> None:
        if self.__client is not None:
            del self.__client

    @property
    def url(self) -> str:
        return f"{self.__http_schema}://{self.__host}:{self.__port}"

    @handle_response
    def get(self, sub_url: str) -> Response:
        url = f"{self.url}/{sub_url}"
        logger.debug(f'GET {url}')
        return self.__client.get(url=url)

    @handle_response
    def post(self, sub_url: str, json_data: dict = None) -> Response:
        url = f"{self.url}/{sub_url}"
        logger.debug(f'POST {url} {json_data}')
        return self.__client.post(url=url, json=json_data)


class PrestoRest(Rest):
    PORT = 8080

    @property
    def url(self) -> str:
        return f"{super(PrestoRest, self).url}/v1"


class VaradaRest(Rest):
    PORT = 8088

    @property
    def url(self) -> str:
        return f"{super(VaradaRest, self).url}/v1/ext/varada"
