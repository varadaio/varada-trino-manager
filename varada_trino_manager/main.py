from .utils import logger
from click import option, group
from .commands import commands_groups
from logging import INFO, DEBUG, StreamHandler


@option("-v", "--verbose", is_flag=True, default=False, help="Be more verbose")
@group()
def main(verbose):
    """
    Varada trino manager
    """
    for handler in logger.handlers:
        if type(handler) == StreamHandler:
            handler.setLevel(DEBUG if verbose else INFO)


for command_group in commands_groups:
    main.add_command(command_group)

if __name__ == "__main__":
    main()
