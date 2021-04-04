from os import execv, makedirs
from os.path import basename, dirname, join as path_join
from typing import List, Tuple
from .constants import Common
from .connections import SSH, SFTP
from subprocess import check_output
from .configuration import get_config, Connection
from concurrent.futures import ThreadPoolExecutor, Future


def ssh_execute(command: str, con: Connection) -> str:
    if con.with_bastion:
        with SSH(
            host=con.bastion_hostname,
            port=con.bastion_port,
            user=con.bastion_username,
        ) as bastion:
            sock = bastion.get_channel(target_host=con.hostname, target_port=con.port)
            with SSH(
                host=con.hostname, port=con.port, user=con.username, sock=sock
            ) as client:
                return client.execute(command=command)
    else:
        with SSH(host=con.hostname, port=con.port, user=con.username) as client:
            return client.execute(command=command)


def ssh_session(node: str) -> None:
    config = get_config()
    con = config.get_connection_by_name(node)
    ssh_executable = check_output("which ssh".split(" ")).decode().strip()
    if con.with_bastion:
        args = f"{ssh_executable} {con.bastion_username}@{con.bastion_hostname} -p {con.bastion_port} {Common.SSH_ARGS_AGENT_FORWARDING} 'ssh {con.username}@{con.hostname} -p {con.port}'"
    else:
        args = f"{ssh_executable} {con.username}@{con.hostname} -p {con.port} {Common.SSH_ARGS_AGENT_FORWARDING}"
    print(f"Connecting to {con}")
    execv(ssh_executable, args.split(" "))


def download(con: Connection, remote_file_path: str, local_file_path: str) -> None:
    if con.with_bastion:
        with SSH(
            host=con.bastion_hostname,
            port=con.bastion_port,
            user=con.bastion_username,
        ) as bastion:
            sock = bastion.get_channel(target_host=con.hostname, target_port=con.port)
            with SFTP(
                host=con.hostname, port=con.port, user=con.username, sock=sock
            ) as client:
                return client.get(
                    remote_file_path=remote_file_path, local_file_path=local_file_path
                )
    else:
        with SFTP(host=con.hostname, port=con.port, user=con.username) as client:
            return client.get(
                remote_file_path=remote_file_path, local_file_path=local_file_path
            )


def parallel_ssh_execute(command: str) -> List[Tuple[Future, str]]:
    config = get_config()
    with ThreadPoolExecutor(max_workers=config.number_of_nodes) as tpx:
        tasks = [
            (
                tpx.submit(ssh_execute, con=connection, command=command),
                connection.hostname,
            )
            for connection in config.iter_connections()
        ]
    return tasks


def parallel_download(
    remote_file_path: str, local_dir_path: str
) -> List[Tuple[Future, str]]:
    config = get_config()
    with ThreadPoolExecutor(max_workers=config.number_of_nodes) as tpx:
        tasks = []
        for con in config.iter_connections():
            local_file_path = path_join(
                local_dir_path, f"{con.role}-{con.hostname}", basename(remote_file_path)
            )
            makedirs(dirname(dirname(local_file_path)), exist_ok=True)
            tasks.append(
                (
                    tpx.submit(
                        download,
                        con=con,
                        remote_file_path=remote_file_path,
                        local_file_path=local_file_path,
                    ),
                    con.hostname,
                )
            )
    return tasks
