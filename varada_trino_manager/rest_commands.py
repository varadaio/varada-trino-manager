from .connections import Rest


class RestCommands:
    @staticmethod
    def info(client: Rest):
        return client.get("info").json()
