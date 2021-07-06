from .constants import Paths
from json import loads, load
from logging.config import dictConfig
from logging import Logger, getLogger
from os.path import exists, dirname, abspath, join


def read_file(file_path: str) -> str:
    if not exists(file_path):
        raise FileNotFoundError(f"{file_path} doesn't exists")
    with open(file=file_path, mode="rb") as f:
        return f.read()


def read_file_as_json(file_path: str) -> dict:
    return loads(read_file(file_path=file_path))


def session_props_to_dict(properties: str) -> dict:
    return {
        key: value for obj in properties.split(",") for key, value in [obj.split("=")]
    }


def init_logger() -> Logger:
    config_path = join(dirname(abspath(__file__)), "logging.json")
    with open(config_path) as f:
        logger_config = load(f)
    Paths.logs_path.mkdir(parents=True, exist_ok=True)
    logger_config["handlers"]["file"]["filename"] = f"{Paths.logs_path}/vtm.log"
    dictConfig(logger_config)
    return getLogger("vtm")


logger = init_logger()
