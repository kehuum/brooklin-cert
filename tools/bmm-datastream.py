#!/usr/bin/env python3
"""This is a script which uses brooklin-tool to perform datastream CRUD operations for MirrorMaker streams"""

import argparse
import logging
import subprocess
import sys

from common import csv

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
log = logging.getLogger()

BASE_TOOL_COMMAND = 'brooklin-tool datastream'

LIST_COMMAND = 'list'
CREATE_COMMAND = 'create'
DELETE_COMMAND = 'delete'
STOP_COMMAND = 'stop'
PAUSE_COMMAND = 'pause'
RESUME_COMMAND = 'resume'
RESTART_COMMAND = 'restart'


def parse_args():
    parser = argparse.ArgumentParser(description='Perform all datastream CRUD operations using brooklin-tool')

    # Create a common parser which can be shared by all sub-commands
    common_parser = argparse.ArgumentParser(add_help=False)

    # Add all the optional arguments for the common parser
    common_parser.add_argument('--debug', action='store_true')
    common_parser.add_argument('--dryrun', action='store_true', help='Prints the command without running it')

    # Add all the required arguments for the common parser
    required_common = common_parser.add_argument_group('required arguments')
    required_common.add_argument('--fabric', '-f', help='Brooklin cluster fabric', required=True)
    required_common.add_argument('--tag', '-t', help='Brooklin cluster tag', required=True)

    # Choose CRUD command to run with relevant details
    subcommands = parser.add_subparsers(help='Help for supported commands')
    subcommands.dest = 'cmd'
    subcommands.required = True

    def add_subparser(name, help):
        return subcommands.add_parser(name, help=help, parents=[common_parser])

    list_command_parser(add_subparser)
    create_command_parser(add_subparser)
    delete_command_parser(add_subparser)
    stop_command_parser(add_subparser)
    pause_command_parser(add_subparser)
    resume_command_parser(add_subparser)
    restart_command_parser(add_subparser)

    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    if args.cmd == CREATE_COMMAND:
        # Destination fabric is picked up from the fabric if not set
        if not args.destinationfabric:
            args.destinationfabric = args.fabric
        # Validate that at least one principal is passed
        if not args.users and not args.groups and not args.applications:
            parser.error('At least one of --users/--groups/--applications just be specified')

    return args


def list_command_parser(add_parser):
    # All the arguments for the list sub-command
    list_command = add_parser(LIST_COMMAND, help='List all datastreams in a given Brooklin cluster')
    list_command.set_defaults(cmd=LIST_COMMAND)


def create_command_parser(add_parser):
    # All the arguments for the create sub-command
    create_command = add_parser(CREATE_COMMAND, help='Create a datastream')

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
    create_command_optional_group.add_argument('--applications', type=csv, help='Comma separated list of applications')
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
    create_command.set_defaults(cmd=CREATE_COMMAND)


def delete_command_parser(add_parser):
    # All the arguments for the delete sub-command
    delete_command = add_parser(DELETE_COMMAND, help='Delete a datastream')

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
    delete_command.set_defaults(cmd=DELETE_COMMAND)


def stop_command_parser(add_parser):
    # All the arguments for the stop sub-command
    stop_command = add_parser(STOP_COMMAND, help='Stop a datastream')

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
    stop_command.set_defaults(cmd=STOP_COMMAND)


def pause_command_parser(add_parser):
    # All the arguments for the pause sub-command
    pause_command = add_parser(PAUSE_COMMAND, help='Pause a datastream')

    # Add all the required arguments for the pause sub-command
    pause_command_group = pause_command.add_argument_group('required arguments')
    pause_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
    pause_command_group.add_argument('--cert', '-c', required=True,
                                     help='Certificate to use to for 2FA with brooklin-tool')
    pause_command_group.add_argument('--message', '-m', required=True, help='Reason for pausing the stream')

    # Add all the optional arguments for the pause sub-command
    pause_command_optional_group = pause_command.add_argument_group('optional arguments')
    pause_command_optional_group.add_argument('--force', action='store_true',
                                              help='Skip the interactive prompt to perform the force pause')
    pause_command_optional_group.add_argument('--topic', required=False, help='Topic to pause')
    pause_command_optional_group.add_argument('--partitions', required=False, type=csv,
                                              help='Comma separated list of partitions to pause or * for all'
                                                   ' partitions')
    pause_command.set_defaults(cmd=PAUSE_COMMAND)


def resume_command_parser(add_parser):
    # All the arguments for the resume sub-command
    resume_command = add_parser(RESUME_COMMAND, help='Resume a datastream')

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
    resume_command.set_defaults(cmd=RESUME_COMMAND)


def restart_command_parser(add_parser):
    # All the arguments for the restart sub-command
    restart_command = add_parser(RESTART_COMMAND, help='Restart a datastream (stop + start)')

    # Add all the required arguments for the restart sub-command
    restart_command_group = restart_command.add_argument_group('required arguments')
    restart_command_group.add_argument('--name', '-n', required=True, help='Datastream name')
    restart_command_group.add_argument('--cert', '-c', required=True,
                                       help='Certificate to use to for 2FA with brooklin-tool')

    # Add all the optional arguments for the restart sub-command
    restart_command_optional_group = restart_command.add_argument_group('optional arguments')
    restart_command_optional_group.add_argument('--wait', type=int, required=False,
                                                help='Time in seconds to wait between stop and resume')
    restart_command.set_defaults(cmd=RESTART_COMMAND)


def join_csv_values(values_list):
    return ','.join(values_list)


def build_datastream_list_command(args):
    list_command = f'{BASE_TOOL_COMMAND} {LIST_COMMAND} ' \
                   f'-f {args.fabric} ' \
                   f'-t {args.tag}'
    return list_command


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


def build_datastream_pause_command(args):
    pause_command = f'{BASE_TOOL_COMMAND} {PAUSE_COMMAND} ' \
                    f'-f {args.fabric} ' \
                    f'--tags {args.tag} ' \
                    f'-n {args.name} ' \
                    f'--cert {args.cert} ' \
                    f'--message "{args.message}" '

    # The remaining are optional, so must check if they exist
    if args.topic:
        pause_command += f'--topic {args.topic} '
    if args.partitions:
        pause_command += f'--partitions {join_csv_values(args.partitions)} '
    if args.force:
        pause_command += f'--force'

    return pause_command


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
        resume_command += f'--partitions {join_csv_values(args.partitions)}'

    return resume_command


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


def run_command(command, timeout):
    print("Running command")
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return process.wait(timeout), process.stdout.read().decode('utf-8')


def fail(message):
    log.error(message)
    sys.exit(1)


def main():
    args = parse_args()

    commands = {
        LIST_COMMAND: build_datastream_list_command,
        CREATE_COMMAND: build_datastream_create_command,
        DELETE_COMMAND: build_datastream_delete_command,
        STOP_COMMAND: build_datastream_stop_command,
        PAUSE_COMMAND: build_datastream_pause_command,
        RESUME_COMMAND: build_datastream_resume_command,
        RESTART_COMMAND: build_datastream_restart_command
    }

    build_command_fn = commands[args.cmd]
    cmd = build_command_fn(args)

    print(cmd)
    if not args.dryrun:
        try:
            return_code, output = run_command(cmd, 60000)
            print(output)
        except subprocess.TimeoutExpired:
            fail("Command timed out")
        else:
            if return_code != 0:
                fail(f"Command returned non-zero status code and printed {output}")


if __name__ == '__main__':
    main()
