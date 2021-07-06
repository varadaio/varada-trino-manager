from click import option, Choice

TARGET_MAP = {
    None: (False, False),
    "coordinator": (True, False),
    "workers": (False, True),
}


def add_options(options):
    def _add_options(func):
        for opt in reversed(options):
            func = opt(func)
        return func

    return _add_options


NODES_OPTIONS = [
    option(
        "-t",
        "--target",
        type=Choice(["coordinator", "workers"]),
        default=None,
        help="run command on either coordinator or workers, if empty will run on all the nodes",
    )
]
