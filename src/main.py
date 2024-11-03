import logging
import typing as t

import click
from click import Command

from src.command import ingest, process

logger = logging.getLogger(__name__)


# Taken from https://stackoverflow.com/a/66792281
class MutuallyExclusiveCommandGroup(click.Group):
    def __init__(self, *args, **kwargs):
        kwargs["chain"] = True
        self.mutually_exclusive = []
        super().__init__(*args, **kwargs)

    def command(self, *args, mutually_exclusive=False, **kwargs):
        """Track the commands marked as mutually exclusive"""
        super_decorator = super().command(*args, **kwargs)

        def decorator(f):
            command = super_decorator(f)
            if mutually_exclusive:
                self.mutually_exclusive.append(command)
            return command

        return decorator

    def add_command(
        self, cmd: Command, name: t.Optional[str] = None, mutually_exclusive=False
    ) -> None:
        super().add_command(cmd, name)
        if mutually_exclusive:
            self.mutually_exclusive.append(cmd)

    def resolve_command(self, ctx, args):
        """Hook the command resolving and verify mutual exclusivity"""
        cmd_name, cmd, args = super().resolve_command(ctx, args)

        # find the commands which are going to be run
        if not hasattr(ctx, "resolved_commands"):
            ctx.resolved_commands = set()
        ctx.resolved_commands.add(cmd_name)

        # if args is empty we have found all the commands to be run
        if not args:
            mutually_exclusive = ctx.resolved_commands & set(
                cmd.name for cmd in self.mutually_exclusive
            )
            if len(mutually_exclusive) > 1:
                raise click.UsageError(
                    f"Illegal usage: commands: `{', '.join(mutually_exclusive)}` are mutually exclusive"
                )

        return cmd_name, cmd, args

    def get_help(self, ctx):
        """Extend the short help for the mutually exclusive commands"""
        for cmd in self.mutually_exclusive:
            exclusive_cmds = set(self.mutually_exclusive)
            if not cmd.short_help:
                commands = ", ".join(
                    command.name
                    for command in exclusive_cmds
                    if command.name != cmd.name
                )
                cmd.short_help = f"mutually exclusive with: {commands}"
        return super().get_help(ctx)


@click.group(cls=MutuallyExclusiveCommandGroup, chain=True)
def main():
    pass


@click.command()
def create_dataset() -> None:
    # TODO: Create dataset in the gold layer
    pass


@click.command()
def predict() -> None:
    # TODO: Predict late book return
    pass


main.add_command(ingest, mutually_exclusive=True)
main.add_command(process, mutually_exclusive=True)
main.add_command(create_dataset, mutually_exclusive=True)
main.add_command(predict, mutually_exclusive=True)

if __name__ == "__main__":
    main()
