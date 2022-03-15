from ..infra.call_home_methods import run
from click import group, Path, echo, option, argument
from os.path import dirname, abspath, join


@group()
def call_home():
    """
    call home related commands
    """
    pass


@option(
    "--config-template",
    "-t",
    is_flag=True,
    help="print config json template",
)
@option(
    "--config-json",
    "-c",
    type=Path(exists=True),
    help="create folder with logs & graphs",
)
@call_home.command()
def create_dumps(config_template, config_json):
    """
    create folder with logs & graphs
    """
    if config_template is False and config_json is None:
        print("required -t (config template) or -c with config file path")
        return
    if config_template:
        sample_config_path = join(dirname(abspath(__file__)), "../infra/call_home_sample.json")
        with open(sample_config_path) as f:
            echo(f.read())

    if config_json:
        run(config_json)

