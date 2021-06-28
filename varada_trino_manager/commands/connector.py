from click import option, group, echo, Path as ClickPath
from ..infra.remote import parallel_ssh_execute, parallel_upload


@group()
def connector():
    """
    Connector related commands
    """


@option(
    "-t",
    "--targz-path",
    type=ClickPath(),
    help="Path to targz file contains varada connector",
    required=True,
)
@option(
    "-p",
    "--script-params",
    type=str,
    help="Params to pass to the connector install script",
    default=None,
)
@option(
    "-e",
    "--external-install-script-path",
    type=ClickPath(),
    help="External install script path",
    default=None,
)
@option(
    "-i",
    "--installation-dir",
    type=str,
    help="Remote installation directory, for example - /usr/lib/presto",
    default=None,
)
@option(
    "-u",
    "--user",
    type=str,
    help="User that runs the presto server",
    default=None,
)
@connector.command()
def install(
    targz_path: str,
    script_params: str,
    external_install_script_path: str,
    installation_dir: str,
    user: str,
):
    """
    Install Varada connector
    """
    parallel_upload(
        local_file_path=targz_path, remote_file_path="/tmp/varada-connector.tar.gz"
    )
    if external_install_script_path:
        parallel_upload(
            local_file_path=external_install_script_path,
            remote_file_path="/tmp/external-install.py",
        )
    commands = [
        f"sudo usermod -a -G disk {user if user else '$(whoami)'}",
        "mkdir /tmp/varada-install",
        "sudo mkdir -p /var/lib/presto/workerDB",
        "sudo chmod 777 -R /var/lib/presto/workerDB",
        "tar -zxf /tmp/varada-connector.tar.gz -C /tmp/varada-install",
        f"sudo chmod 777 -R {installation_dir}",
        f"cp -R /tmp/varada-install/varada-connector-350/presto/plugin/varada {installation_dir}/plugin/.",
        "sudo cp -R /tmp/varada-install/varada-connector-350/trc /usr/local/trc",
        "sudo ln -sfn /usr/local/trc/trc_decoder /usr/local/bin/trc_decoder",
        "cd /tmp/varada-install/varada-connector-350",
        f"sudo python3 {'/tmp/external-install.py' if external_install_script_path else '/tmp/varada-install/varada-connector-350/varada/installer.py'} {script_params}",
    ]
    for task, hostname in parallel_ssh_execute("\n".join(commands)):
        echo(f"{hostname}: {task.result()}")
