"""
XXX: placeholder for the main command
"""
import click


@click.command()
@click.option('--name', prompt='Your name', help='The person to greet.')
def sbkt_command(name):
    click.echo(f'Hello {name}!')
