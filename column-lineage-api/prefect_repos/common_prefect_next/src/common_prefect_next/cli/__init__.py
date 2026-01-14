import click

from common_prefect_next.cli.register import (
    register_aws,
    register_data_canvas,
    register_database,
    register_docker,
    register_environment,
    register_thoughtspot,
    register_work_queue_names,
)


@click.group()
def cli() -> None:
    """Main entry point for the CLI"""


@cli.group()
def register() -> None: ...


@register.command()
def aws() -> None:
    register_aws()


@register.command(name="db")
def database() -> None:
    register_database()


@register.command(name="ts")
def thoughtspot() -> None:
    register_thoughtspot()


@register.command(name="env")
def environment() -> None:
    register_environment()


@register.command(name="docker")
def docker() -> None:
    register_docker()


@register.command(name="data_canvas")
def data_canvas() -> None:
    register_data_canvas()


@register.command(name="work_queues")
def work_queues() -> None:
    register_work_queue_names()


if __name__ == "__main__":
    cli()
