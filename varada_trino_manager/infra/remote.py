from click import echo
from typing import Union
from .utils import logger
from .constants import Common
from typing import List, Tuple
from os import execv, makedirs
from traceback import format_exc
from subprocess import check_output
from .configuration import get_config, Connection
from os.path import basename, dirname, join as path_join
from concurrent.futures import ThreadPoolExecutor, Future
from .connections import SSH, SFTP, Rest, Trino, VaradaRest


def rest_execute(con: Connection, rest_client_type: Union[Rest, Trino], func, *args, **kw):
    with rest_client_type(con) as client:
        return func(client, *args, **kw)


def parallel_rest_execute(rest_client_type: Union[Rest, Trino, VaradaRest], func, *args, **kw):
    config = get_config()
    with ThreadPoolExecutor(max_workers=config.number_of_nodes) as tpx:
        tasks = [
            (
                tpx.submit(rest_execute, con=connection, rest_client_type=rest_client_type, func=func, *args, **kw),
                connection.hostname,
            )
            for connection in config.iter_connections()
        ]
    return tasks


def ssh_execute(command: str, con: Connection) -> str:
    try:
        with SSH(con=con) as client:
            return client.execute(command=command)
    except Exception:
        logger.error(format_exc())


def ssh_session(node: str) -> None:
    config = get_config()
    con = config.get_connection_by_name(node)
    ssh_executable = check_output("which ssh".split(" ")).decode().strip()
    args = f"{ssh_executable} {con.username}@{con.hostname} -p {con.port} {Common.SSH_ARGS} {f'-J {con.bastion_username}@{con.bastion_hostname}' if con.with_bastion else ''}"
    logger.info(f"Connecting to {con}")
    execv(ssh_executable, args.split(" "))


def download(con: Connection, remote_file_path: str, local_file_path: str) -> None:
    logger.debug(f"Copying {con}{remote_file_path} {local_file_path}")
    try:
        with SFTP(con=con) as client:
            return client.get(remote_file_path=remote_file_path, local_file_path=local_file_path)
    except Exception:
        logger.error(format_exc())

def upload(con: Connection, local_file_path: str, remote_file_path: str) -> None:
    logger.debug(f"Copying {con}{local_file_path} {remote_file_path}")
    try:
        with SFTP(con=con) as client:
            return client.put(local_file_path=local_file_path, remote_file_path=remote_file_path)
    except Exception:
        logger.error(format_exc())


def parallel_ssh_execute(command: str, coordinator: bool = False, workers: bool = False) -> List[Tuple[Future, str]]:
    config = get_config()
    if coordinator:
        connections = [config.coordinator_connection]
    elif workers:
        connections = config.iter_workers_connections()
    else:
        connections = config.iter_connections()
    with ThreadPoolExecutor(max_workers=config.number_of_nodes) as tpx:
        tasks = [
            (
                tpx.submit(ssh_execute, con=connection, command=command),
                connection.hostname,
            )
            for connection in connections
        ]
    return tasks


def parallel_download(
    remote_file_path: str, local_dir_path: str
) -> List[Tuple[Future, str]]:
    config = get_config()
    tasks = []
    with ThreadPoolExecutor(max_workers=config.number_of_nodes) as tpx:
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


def parallel_upload(local_file_path: str, remote_file_path: str) -> List[Tuple[Future, str]]:
    config = get_config()
    tasks = []
    with ThreadPoolExecutor(max_workers=config.number_of_nodes) as tpx:
        for con in config.iter_connections():
            tasks.append(
                (
                    tpx.submit(
                        upload,
                        con=con,
                        local_file_path=local_file_path,
                        remote_file_path=remote_file_path,
                    ),
                    con.hostname,
                )
            )
    return tasks
