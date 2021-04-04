from json import loads
from os.path import exists


def read_file(file_path: str) -> str:
    if not exists(file_path):
        raise FileNotFoundError(f"{file_path} doesn't exists")
    with open(file=file_path, mode="rb") as f:
        return f.read()


def read_file_as_json(file_path: str) -> dict:
    return loads(read_file(file_path=file_path))
