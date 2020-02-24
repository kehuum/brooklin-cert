#!/usr/bin/env python3
"""This is a script which uses brooklin-tool to perform datastream CRUD operations for MirrorMaker streams"""

import argparse
import json
import logging
import subprocess
import sys

from abc import ABC, abstractmethod
from common import csv, typename

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
log = logging.getLogger()

BASE_TOOL_COMMAND = 'brooklin-tool datastream'

GET_COMMAND = 'get'
LIST_COMMAND = 'list'
CREATE_COMMAND = 'create'
DELETE_COMMAND = 'delete'
STOP_COMMAND = 'stop'
PAUSE_COMMAND = 'pause'
RESUME_COMMAND = 'resume'
RESTART_COMMAND = 'restart'


class ArgumentParser(object):
    """Command line arguments parser

    Encapsulates the logic for creating a command line parser with subparsers
    for all the various verbs. Exposes a method, parse_args(), for performing
    argument parsing and validation.

    """

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Perform all datastream CRUD operations using brooklin-tool')

        # Create a common parser which can be shared by all sub-commands
        self.common_parser = argparse.ArgumentParser(add_help=False)

        # Create a common parser which can be shared by all sub-commands
        self.common_parser = argparse.ArgumentParser(add_help=False)

        # Add all the optional arguments for the common parser
        self.common_parser.add_argument('--debug', action='store_true')
        self.common_parser.add_argument('--dryrun', action='store_true', help='Prints the command without running it')

        # Add all the required arguments for the common parser
        required_common = self.common_parser.add_argument_group('required arguments')
        required_common.add_argument('--fabric', '-f', help='Brooklin cluster fabric', required=True)
        required_common.add_argument('--tag', '-t', help='Brooklin cluster tag', required=True)

        # Choose CRUD command to run with relevant details
        self.subcommands = self.parser.add_subparsers(help='Help for supported commands')
        self.subcommands.dest = 'cmd'
        self.subcommands.required = True

        self.get_command_parser()
        self.list_command_parser()
        self.create_command_parser()
        self.delete_command_parser()
        self.stop_command_parser()
        self.pause_command_parser()
        self.resume_command_parser()
        self.restart_command_parser()

    def parse_args(self):
        args = self.parser.parse_args()

        if args.debug:
            log.setLevel(logging.DEBUG)

        if args.cmd == CREATE_COMMAND:
            # Destination fabric is picked up from the fabric if not set
            if not args.destinationfabric:
                args.destinationfabric = args.fabric
            # Validate that at least one principal is passed
            if not args.users and not args.groups and not args.applications:
                self.parser.error('At least one of --users/--groups/--applications just be specified')
        return args

    def add_subparser(self, name, help):
        return self.subcommands.add_parser(name, help=help, parents=[self.common_parser])

    def get_command_parser(self):
        # All the arguments for the get sub-command
        get_command = self.add_subparser(GET_COMMAND, help='Get a datastream')

        # Add all the required arguments for the get sub-command
        get_command_group = get_command.add_argument_group('required arguments')
        get_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        get_command.set_defaults(cmd=GetDatastream)

    def list_command_parser(self):
        # All the arguments for the list sub-command
        list_command = self.add_subparser(LIST_COMMAND, help='List all datastreams in a given Brooklin cluster')
        list_command.set_defaults(cmd=ListDatastream)

    def create_command_parser(self):
        # All the arguments for the create sub-command
        create_command = self.add_subparser(CREATE_COMMAND, help='Create a datastream')

        # Add all the required arguments for the create sub-command
        create_command_group = create_command.add_argument_group('required arguments')
        create_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        create_command_group.add_argument('--cert', '-c', required=True,
                                          help='Certificate to use to for 2FA with brooklin-tool')
        create_command_group.add_argument('--whitelist', required=True,
                                          help='The whitelist regular expression that describes the set of topics to'
                                               ' mirror')
        create_command_group.add_argument('--sourcefabric', required=True, help='The source fabric to mirror from')
        create_command_group.add_argument('--sourcecluster', required=True, help='The Kafka cluster to mirror from')

        create_command_group.add_argument('--destinationcluster', required=True, help='The Kafka cluster to mirror to')
        create_command_group.add_argument('--jira', required=True,
                                          help='The JIRA ticket (e.g. CM-60690) that contains approval for creating'
                                               ' a Kafka Mirror Maker datastream')

        # Add all the optional arguments for the create sub-command
        create_command_optional_group = create_command.add_argument_group('optional arguments')
        create_command_optional_group.add_argument('--destinationfabric', required=False,
                                                   help='The destination fabric to mirror to (defaults to --fabric)')
        create_command_optional_group.add_argument('--numtasks', type=int, default=1,
                                                   help='The number of Datastream tasks (consumer-producer pairs) to create'
                                                        ' for the mirroring pipeline; Defaults to 1 per host in the'
                                                        ' Brooklin cluster')
        create_command_optional_group.add_argument('--offsetreset',
                                                   help='Creates the datastream with the given auto.offset.reset strategy')
        create_command_optional_group.add_argument('--groupid',
                                                   help='The consumer group for the datastream. Defaults to the'
                                                        'datastream name')
        create_command_optional_group.add_argument('--users', type=csv, help='Comma separated list of users')
        create_command_optional_group.add_argument('--groups', type=csv, help='Comma separated list of groups')
        create_command_optional_group.add_argument('--applications', type=csv,
                                                   help='Comma separated list of applications')
        create_command_optional_group.add_argument('--partitionmanaged', action='store_true',
                                                   help='Enable partition management in kafka consumer')
        create_command_optional_group.add_argument('--passthrough', action='store_true',
                                                   help='Enable passthrough compression in kafka consumer and producer')
        create_command_optional_group.add_argument('--identity', action='store_true',
                                                   help='Enable data from source partition N to go to destination'
                                                        ' partition N')
        create_command_optional_group.add_argument('--topiccreate', action='store_true',
                                                   help='Enable topic creation ability in BMM')
        create_command_optional_group.add_argument('--metadata', action='append',
                                                   help='Metadata property overrides defined as key value pairs separated '
                                                        'by ":". This can be repeated to specify multiple properties')
        create_command.set_defaults(cmd=CreateDatastream)

    def delete_command_parser(self):
        # All the arguments for the delete sub-command
        delete_command = self.add_subparser(DELETE_COMMAND, help='Delete a datastream')

        # Add all the required arguments for the delete sub-command
        delete_command_group = delete_command.add_argument_group('required arguments')
        delete_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        delete_command_group.add_argument('--cert', '-c', required=True,
                                          help='Certificate to use to for 2FA with brooklin-tool')
        delete_command_group.add_argument('--jira', required=True,
                                          help='The JIRA ticket (e.g. CM-60690) that contains approval for deleting'
                                               ' a Kafka Mirror Maker datastream')

        # Add all the optional arguments for the delete sub-command
        delete_command_optional_group = delete_command.add_argument_group('optional arguments')
        delete_command_optional_group.add_argument('--force', action='store_true',
                                                   help='Skip the interactive prompt to perform the force delete')
        delete_command.set_defaults(cmd=DeleteDatastream)

    def stop_command_parser(self):
        # All the arguments for the stop sub-command
        stop_command = self.add_subparser(STOP_COMMAND, help='Stop a datastream')

        # Add all the required arguments for the stop sub-command
        stop_command_group = stop_command.add_argument_group('required arguments')
        stop_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        stop_command_group.add_argument('--cert', '-c', required=True,
                                        help='Certificate to use to for 2FA with brooklin-tool')
        stop_command_group.add_argument('--message', '-m', required=True, help='Reason for stopping the stream')

        # Add all the optional arguments for the stop sub-command
        stop_command_optional_group = stop_command.add_argument_group('optional arguments')
        stop_command_optional_group.add_argument('--force', action='store_true',
                                                 help='Skip the interactive prompt to perform the force stop')
        stop_command.set_defaults(cmd=StopDatastream)

    def pause_command_parser(self):
        # All the arguments for the pause sub-command
        pause_command = self.add_subparser(PAUSE_COMMAND, help='Pause a datastream')

        # Add all the required arguments for the pause sub-command
        pause_command_group = pause_command.add_argument_group('required arguments')
        pause_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        pause_command_group.add_argument('--message', '-m', required=True, help='Reason for pausing the stream')

        # Add all the optional arguments for the pause sub-command
        pause_command_optional_group = pause_command.add_argument_group('optional arguments')
        pause_command_optional_group.add_argument('--force', action='store_true',
                                                  help='Skip the interactive prompt to perform the force pause')
        pause_command_optional_group.add_argument('--topic', required=False, help='Topic to pause')
        pause_command_optional_group.add_argument('--partitions', required=False, type=csv,
                                                  help='Comma separated list of partitions to pause or * for all'
                                                       ' partitions')
        pause_command.set_defaults(cmd=PauseDatastream)

    def resume_command_parser(self):
        # All the arguments for the resume sub-command
        resume_command = self.add_subparser(RESUME_COMMAND, help='Resume a datastream')

        # Add all the required arguments for the resume sub-command
        resume_command_group = resume_command.add_argument_group('required arguments')
        resume_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        resume_command_group.add_argument('--cert', '-c', required=True,
                                          help='Certificate to use to for 2FA with brooklin-tool')

        # Add all the optional arguments for the resume sub-command
        resume_command_optional_group = resume_command.add_argument_group('optional arguments')
        resume_command_optional_group.add_argument('--topic', required=False, help='Topic to resume')
        resume_command_optional_group.add_argument('--partitions', required=False, type=csv,
                                                   help='Comma separated list of partitions to resume or * for all'
                                                        ' partitions')
        resume_command.set_defaults(cmd=ResumeDatastream)

    def restart_command_parser(self):
        # All the arguments for the restart sub-command
        restart_command = self.add_subparser(RESTART_COMMAND, help='Restart a datastream (stop + start)')

        # Add all the required arguments for the restart sub-command
        restart_command_group = restart_command.add_argument_group('required arguments')
        restart_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        restart_command_group.add_argument('--cert', '-c', required=True,
                                           help='Certificate to use to for 2FA with brooklin-tool')

        # Add all the optional arguments for the restart sub-command
        restart_command_optional_group = restart_command.add_argument_group('optional arguments')
        restart_command_optional_group.add_argument('--wait', type=int, required=False,
                                                    help='Time in seconds to wait between stop and resume')
        restart_command.set_defaults(cmd=RestartDatastream)


class BrooklinToolCommandBuilder(object):
    """brooklin-tool command generator"""

    @staticmethod
    def build_datastream_get_command(args):
        return f'{BASE_TOOL_COMMAND} {GET_COMMAND} ' \
               f'-n {args.name} ' \
               f'-f {args.fabric} ' \
               f'-t {args.tag} ' \
               f'--json'

    @staticmethod
    def build_datastream_list_command(args):
        return f'{BASE_TOOL_COMMAND} {LIST_COMMAND} ' \
               f'-f {args.fabric} ' \
               f'-t {args.tag}'

    @staticmethod
    def build_datastream_create_command(args):
        create_command = f'{BASE_TOOL_COMMAND} {CREATE_COMMAND} mirrormaker ' \
                         f'-f {args.fabric} ' \
                         f'--tag {args.tag} ' \
                         f'-n {args.name} ' \
                         f'--whitelist {args.whitelist} ' \
                         f'--source-fabric {args.sourcefabric} ' \
                         f'--source-cluster {args.sourcecluster} ' \
                         f'--destination-fabric {args.destinationfabric} ' \
                         f'--destination-cluster {args.destinationcluster} ' \
                         f'--jira {args.jira} ' \
                         f'--num-tasks {args.numtasks} ' \
                         f'--cert {args.cert} '

        join_csv_values = BrooklinToolCommandBuilder.join_csv_values

        # The remaining are optional, so must check if they exist
        if args.users:
            create_command += f'--users {join_csv_values(args.users)} '
        if args.groups:
            create_command += f'--groups {join_csv_values(args.groups)} '
        if args.applications:
            create_command += f'--applications {join_csv_values(args.applications)} '
        if args.offsetreset:
            create_command += f'--offset-reset {args.offsetreset} '
        if args.groupid:
            create_command += f'--group-id {args.groupid} '
        if args.partitionmanaged:
            create_command += f'--partitionManaged '
        if args.passthrough:
            create_command += f'--passthrough '
        if args.identity:
            create_command += f'--identity '
        if args.topiccreate:
            create_command += f'--createTopic '
        if args.metadata:
            for metadata in args.metadata:
                create_command += f'--metadata {metadata} '
        return create_command

    @staticmethod
    def build_datastream_delete_command(args):
        delete_command = f'{BASE_TOOL_COMMAND} {DELETE_COMMAND} ' \
                         f'-f {args.fabric} ' \
                         f'--tags {args.tag} ' \
                         f'-n {args.name} ' \
                         f'--jira {args.jira} ' \
                         f'--cert {args.cert} '

        # The remaining are optional, so must check if they exist
        if args.force:
            delete_command += f'--force'

        return delete_command

    @staticmethod
    def build_datastream_stop_command(args):
        stop_command = f'{BASE_TOOL_COMMAND} {STOP_COMMAND} ' \
                       f'-f {args.fabric} ' \
                       f'--tags {args.tag} ' \
                       f'-n {args.name} ' \
                       f'--cert {args.cert} ' \
                       f'--message "{args.message}" '

        # The remaining are optional, so must check if they exist
        if args.force:
            stop_command += f'--force'

        return stop_command

    @staticmethod
    def build_datastream_pause_command(args):
        pause_command = f'{BASE_TOOL_COMMAND} {PAUSE_COMMAND} ' \
                        f'-f {args.fabric} ' \
                        f'--tags {args.tag} ' \
                        f'-n {args.name} ' \
                        f'--message "{args.message}" '

        # The remaining are optional, so must check if they exist
        if args.topic:
            pause_command += f'--topic {args.topic} '
        if args.partitions:
            pause_command += f'--partitions {BrooklinToolCommandBuilder.join_csv_values(args.partitions)} '
        if args.force:
            pause_command += f'--force'

        return pause_command

    @staticmethod
    def build_datastream_resume_command(args):
        resume_command = f'{BASE_TOOL_COMMAND} {RESUME_COMMAND} ' \
                         f'-f {args.fabric} ' \
                         f'--tags {args.tag} ' \
                         f'-n {args.name} ' \
                         f'--cert {args.cert} '

        # The remaining are optional, so must check if they exist
        if args.topic:
            resume_command += f'--topic {args.topic} '
        if args.partitions:
            resume_command += f'--partitions {BrooklinToolCommandBuilder.join_csv_values(args.partitions)}'

        return resume_command

    @staticmethod
    def build_datastream_restart_command(args):
        restart_command = f'{BASE_TOOL_COMMAND} {RESTART_COMMAND} ' \
                          f'-f {args.fabric} ' \
                          f'--tags {args.tag} ' \
                          f'-n {args.name} ' \
                          f'--cert {args.cert} '

        # The remaining are optional, so must check if they exist
        if args.wait:
            restart_command += f'--wait {args.wait}'

        return restart_command

    @staticmethod
    def join_csv_values(values_list):
        return ','.join(values_list)


class Datastream(object):
    """Represents a Brooklin datastream

    Exposes utility methods for Brooklin datastreams which are represented
    as plain Python dictionaries obtained from deserializing datastreams
    JSON representation

    """

    def __init__(self, datastream: dict):
        self.datastream = datastream

    def __str__(self):
        return json.dumps(self.datastream, indent=4)

    def get_status(self):
        return self.datastream.get('Status')

    def status_equals(self, status):
        return self.get_status() == status

    def is_ready(self):
        return self.status_equals('READY')

    def is_paused(self):
        return self.status_equals('PAUSED')

    def is_stopped(self):
        return self.status_equals('STOPPED')


class DatastreamCommandError(Exception):
    """Parent exception for all errors encountered when executing datastream commands (e.g. create, delete)"""

    def __init__(self, message, cause=None):
        self._message = message
        self._cause = cause

    @property
    def message(self):
        return self._message

    @property
    def cause(self):
        return self._cause

    def __str__(self):
        s = f"Message: {self.message}"
        if self.cause:
            s += f"\nCause: {typename(self.cause)}: {self.cause}"
        return s


class DatastreamCommandFailedError(DatastreamCommandError):
    """Exception raised when a datastream command (e.g. create, delete) fails"""
    pass


class ShellCommandTimeoutError(DatastreamCommandError):
    """Exception raised when a shell command (typically brooklin-tool) takes too long to execute"""

    def __init__(self, command, cause=None):
        super().__init__(f'Time out elapsed waiting for command to complete: {command}', cause)


class ShellCommandFailedError(DatastreamCommandError):
    """Exception raised when a shell command (typically brooklin-tool) completes with nonzero exit status"""

    def __init__(self, command, returncode, stderr):
        super().__init__(f'Command failed with exit code {returncode}: {command} \n'
                         f'Output:\n'
                         f'{stderr.read().decode("utf-8")}')


class NoSuchDatastreamError(DatastreamCommandError):
    """Exception raised when a datastream is not found"""

    def __init__(self, datastream, cause=None):
        super().__init__(f'Datastream not found: {datastream}', cause)


class DatastreamCommand(object):
    """Base class of all datastream commands

    It serves two main purposes:

        - It gives all datastream commands access to the parsed arguments
        of the datastream operation requested by the user.

        - It provides two basic APIs for all datastream commands, execute()
        and validate(). Each command may choose to implement either one of
        these APIs or both.

    """

    def __init__(self, args):
        self.args = args

    def execute(self):
        """Execute the datastream command"""
        pass

    def validate(self):
        """Validate the side-effects of executing the datastream command"""
        pass

    @staticmethod
    def run_command(command, timeout=60):
        """A utility for executing shell commands"""

        print(f"Running command: {command}")
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            returncode = process.wait(timeout)
        except subprocess.TimeoutExpired as err:
            raise ShellCommandTimeoutError(command, err)
        else:
            if returncode != 0:
                raise ShellCommandFailedError(command, returncode, process.stderr)
            return process.stdout.read().decode('utf-8')


class SimpleDatastreamCommand(DatastreamCommand, ABC):
    """Abstract base class of all datastream commands that map directly to brooklin-tool commands

    Extenders provide this base with the brooklin-tool commands they want to execute by
    overriding the command(self) @property. This base then takes care of executing these
    commands on the shell, returning their outputs, and raising errors in case of failures.

     """

    def execute(self):
        return DatastreamCommand.run_command(self.command)

    @property
    @abstractmethod
    def command(self):
        return ''


class GetDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream get"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_get_command(self.args)

    def execute(self) -> Datastream:
        output = super().execute()
        try:
            return Datastream(json.loads(output))
        except json.JSONDecodeError as err:
            raise NoSuchDatastreamError(self.args.name, err)


class ListDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream list"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_list_command(self.args)


class CreateDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream create"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_create_command(self.args)

    def validate(self):
        """Validate datastream exists and its status is READY"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_ready():
            raise DatastreamCommandFailedError(f"Datastream {self.args.name}' created but its status is: "
                                               f"{datastream.get_status()}")


class DeleteDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream delete"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_delete_command(self.args)

    def validate(self):
        """Validate datastream does not exist"""
        get_command = GetDatastream(self.args)
        try:
            datastream = get_command.execute()
        except NoSuchDatastreamError:
            pass
        else:
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not deleted successfully \n'
                                               f'Found: \n'
                                               f'{datastream}')


class StopDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream stop"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_stop_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is STOPPED"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_stopped():
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not stopped successfully; '
                                               f'its status is: {datastream.get_status()}')


class PauseDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream pause"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_pause_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is PAUSED"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_paused():
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not paused successfully; '
                                               f'its status is: {datastream.get_status()}')


class ResumeDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream resume"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_resume_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is READY"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_ready():
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not resumed successfully; '
                                               f'its status is: {datastream.get_status()}')


class RestartDatastream(SimpleDatastreamCommand):
    """Executes brooklin-tool datastream restart"""

    @property
    def command(self):
        return BrooklinToolCommandBuilder.build_datastream_restart_command(self.args)

    def validate(self):
        """Validate datastream still exists and its status is READY"""
        get_command = GetDatastream(self.args)
        datastream = get_command.execute()
        if not datastream.is_ready():
            raise DatastreamCommandFailedError(f'Datastream {self.args.name} was not restarted successfully; '
                                               f'its status is: {datastream.get_status()}')


def fail(message):
    """Prints message to stderr and exits with an error code"""
    print(message, file=sys.stderr)
    sys.exit(1)


def main():
    # parse command line arguments
    parser = ArgumentParser()
    args = parser.parse_args()

    # retrieve the DatastreamCommand type associated
    # with the parsed arguments and instantiate it
    datastream_command_type: type = args.cmd
    datastream_command: DatastreamCommand = datastream_command_type(args)
    datastream_command_typename = typename(datastream_command)

    # execute command
    try:
        output = datastream_command.execute()
    except DatastreamCommandError as err:
        fail(f"Encountered an error executing {datastream_command_typename} command: \n"
             f"{err}")
    else:
        if output:
            print(output)

    # validate command
    try:
        datastream_command.validate()
    except DatastreamCommandError as err:
        fail(f"Encountered an error validating {datastream_command_typename} command: \n"
             f"{err}")


if __name__ == '__main__':
    main()
