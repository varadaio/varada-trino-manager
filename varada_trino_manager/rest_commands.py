from .connections import Rest, Trino


def return_single_value(func):
    def return_single_value_wrapper(*args, **kw):
        return func(*args, **kw)[0][0]

    return return_single_value_wrapper


class RestCommands:
    @staticmethod
    def info(client: Rest):
        return client.get("info").json()

    @return_single_value
    def is_all_nodes_connected(client: Trino) -> bool:
        return client.execute(
            query="with a as (select count(*) as a1 from system.runtime.nodes where state='active'), b as (select count(*) as b1 from system.runtime.nodes) select a.a1=b.b1 from a,b"
        )
