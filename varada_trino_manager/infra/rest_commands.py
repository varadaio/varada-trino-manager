from json import dump
from .configuration import Connection
from .connections import Rest, APIClient, ExtendedRest, VaradaRest


def return_single_value(func):
    def return_single_value_wrapper(*args, **kw):
        return func(*args, **kw)[0][0]

    return return_single_value_wrapper


class RestCommands:
    @staticmethod
    def info(client: Rest):
        return client.get("info").json()

    @staticmethod
    def jstack(client: Rest):
        return client.get("thread").json()

    @staticmethod
    @return_single_value
    def is_all_nodes_connected(client: APIClient) -> bool:
        result, _ = client.execute(
            query="with a as (select count(*) as a1 from system.runtime.nodes where state='active'), b as (select count(*) as b1 from system.runtime.nodes) select a.a1=b.b1 from a,b"
        )
        return result

    @staticmethod
    def save_query_json(con: Connection, dest_dir: str, query_id: str):
        with ExtendedRest(con=con) as trino_rest, open(f'{dest_dir}/{query_id}.json', 'w') as fd:
            data = trino_rest.query_json(query_id=query_id).json()
            dump(data, fd, indent=2)

    @staticmethod
    def dev_log(client: VaradaRest, msg: str):
        client.log(msg=msg)
