from os import makedirs
from os.path import exists, dirname
from paramiko.channel import Channel
from logging import Logger, getLogger
from paramiko.transport import Transport
from paramiko.sftp_client import SFTPClient
from paramiko import AutoAddPolicy, SSHClient


class SSH:
    LOCALHOST: str = "127.0.0.1"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        sock: Channel = None,
        logger: Logger = None,
    ):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__sock = sock
        self.__client = SSHClient()
        self.__transport = None
        self.__logger = getLogger(__name__) if logger is None else logger

    def connect(self):
        self.__client.set_missing_host_key_policy(AutoAddPolicy)
        self.__client.connect(
            hostname=self.__host,
            port=self.__port,
            username=self.__user,
            sock=self.__sock,
            allow_agent=True,
        )

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
        _, stdout, _ = self.__client.exec_command(command=command)
        return stdout.read().decode()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__client.close()


class SFTP(SSH):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        sock: Channel = None,
        logger: Logger = None,
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
