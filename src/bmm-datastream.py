#!/usr/bin/env python3
"""This is a script which uses brooklin-tool to perform datastream CRUD operations for MirrorMaker streams"""

import argparse
import logging
import sys

from testlib.brooklin.tool import CREATE_COMMAND, CreateDatastream, DELETE_COMMAND, DatastreamCommand, \
    DatastreamCommandError, DeleteDatastream, GET_COMMAND, GetDatastream, LIST_COMMAND, ListDatastream, PAUSE_COMMAND, \
    PauseDatastream, RESTART_COMMAND, RESUME_COMMAND, RestartDatastream, ResumeDatastream, STOP_COMMAND, \
    StopDatastream, UPDATE_COMMAND, UpdateDatastream
from testlib.core.utils import typename, csv

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger()


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
        self.update_command_parser()
        self.stop_command_parser()
        self.pause_command_parser()
        self.resume_command_parser()
        self.restart_command_parser()

    def parse_args(self):
        args = self.parser.parse_args()

        if args.debug:
            log.setLevel(logging.DEBUG)

        if args.cmd is CreateDatastream:
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
        create_command_group.add_argument('--scd', required=True, dest='src_cluster_dns',
                                          help='The DNS of the source Kafka cluster to mirror from')

        create_command_group.add_argument('--dcd', dest='dest_cluster_dns', required=True,
                                          help='The DNS of the destination Kafka cluster to mirror to')

        # Add all the optional arguments for the create sub-command
        create_command_optional_group = create_command.add_argument_group('optional arguments')
        create_command_optional_group.add_argument('--jira', default="DATAPIPES-18111",
                                                   help='The JIRA ticket (defaults to DATAPIPES-18111) that contains'
                                                        ' approval for creating a Kafka Mirror Maker datastream')
        create_command_optional_group.add_argument('--numtasks', type=int, default=1,
                                                   help='The number of Datastream tasks (consumer-producer pairs) to'
                                                        ' create for the mirroring pipeline; Defaults to 1 per host in'
                                                        ' the Brooklin cluster')
        create_command_optional_group.add_argument('--offsetreset',
                                                   help='Creates the datastream with the given auto.offset.reset'
                                                        ' strategy')
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

        # Add all the optional arguments for the delete sub-command
        delete_command_optional_group = delete_command.add_argument_group('optional arguments')
        delete_command_optional_group.add_argument('--jira', default="DATAPIPES-18111",
                                                   help='The JIRA ticket (defaults to DATAPIPES-18111) that contains'
                                                        ' approval for deleting a Kafka Mirror Maker datastream')
        delete_command_optional_group.add_argument('--force', action='store_true',
                                                   help='Skip the interactive prompt to perform the force delete')
        delete_command.set_defaults(cmd=DeleteDatastream)

    def update_command_parser(self):
        # All the arguments for the update sub-command
        update_command = self.add_subparser(UPDATE_COMMAND, help='Update a datastream')

        # Add all the required arguments for the update sub-command
        update_command_group = update_command.add_argument_group('required arguments')
        update_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
        update_command_group.add_argument('--cert', '-c', required=True,
                                          help='Certificate to use to for 2FA with brooklin-tool')
        update_command_group.add_argument('--metadata', required=True, action='append',
                                          help='Metadata property overrides defined as key value pairs separated '
                                               'by ":". This can be repeated to specify multiple properties')

        # Add all the optional arguments for the create sub-command
        update_command_optional_group = update_command.add_argument_group('optional arguments')
        update_command_optional_group.add_argument('--jira', default="DATAPIPES-18111",
                                                   help='The JIRA ticket (defaults to DATAPIPES-18111) that contains'
                                                        ' approval for updating a Kafka Mirror Maker datastream')
        update_command_optional_group.add_argument('--newwhitelist', help='The whitelist regular expression that'
                                                                          ' describes the set of topics to mirror')
        update_command_optional_group.add_argument('--force', action='store_true',
                                                   help='Skip the interactive prompt to perform the force update')
        update_command_optional_group.add_argument('--restart', action='store_true',
                                                   help='If specified, will stop and resume the datastream on update')
        update_command_optional_group.add_argument('--wait', type=int, required=False,
                                                   help='Time in seconds to wait between stop and resume')
        update_command.set_defaults(cmd=UpdateDatastream)

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

    # validate command if not in dryrun mode
    if not args.dryrun:
        try:
            datastream_command.validate()
        except DatastreamCommandError as err:
            fail(f"Encountered an error validating {datastream_command_typename} command: \n"
                 f"{err}")


if __name__ == '__main__':
    main()
