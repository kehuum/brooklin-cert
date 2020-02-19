#!/usr/bin/env python3
"""This is a script which uses brooklin-tool to perform datastream CRUD operations for MirrorMaker streams"""

import argparse
import logging
import re
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
log = logging.getLogger()

BASE_TOOL_COMMAND = 'brooklin-tool datastream'


def parse_args():
    def csv(principals):
        if re.match('^[-_.A-z0-9]+(,[-_A-z0-9]+)*,?$', principals):
            return [i for i in principals.split(',') if i]
        raise argparse.ArgumentError('invalid list of principals')

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

    # All the arguments for the list sub-command
    list_command = subcommands.add_parser('list', help='List all datastreams in a given Brooklin cluster',
                                          parents=[common_parser])
    list_command.set_defaults(cmd='list')

    # All the arguments for the create sub-command
    create_command = subcommands.add_parser('create', help='Create a datastream', parents=[common_parser])

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
    create_command.set_defaults(cmd='create')

    # All the arguments for the delete sub-command
    delete_command = subcommands.add_parser('delete', help='Delete a datastream', parents=[common_parser])

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
    delete_command.set_defaults(cmd='delete')

    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    if args.cmd == 'create':
        # Destination fabric is picked up from the fabric if not set
        if not args.destinationfabric:
            args.destinationfabric = args.fabric
        # Validate that at least one principal is passed
        if not args.users and not args.groups and not args.applications:
            parser.error('At least one of --users/--groups/--applications just be specified')

    return args


def join_principals(principal_list):
    return ','.join(principal_list)


def build_datastream_list_command(args):
    list_command = f'{BASE_TOOL_COMMAND} list ' \
                   f'-f {args.fabric} ' \
                   f'-t {args.tag}'
    return list_command


def build_datastream_create_command(args):
    create_command = f'{BASE_TOOL_COMMAND} create mirrormaker ' \
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
        create_command += f'--users {join_principals(args.users)} '
    if args.groups:
        create_command += f'--groups {join_principals(args.groups)} '
    if args.applications:
        create_command += f'--applications {join_principals(args.applications)} '
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
    delete_command = f'{BASE_TOOL_COMMAND} delete ' \
                     f'-f {args.fabric} ' \
                     f'--tags {args.tag} ' \
                     f'-n {args.name} ' \
                     f'--jira {args.jira} ' \
                     f'--cert {args.cert} '

    # The remaining are optional, so must check if they exist
    if args.force:
        delete_command += f'--force'

    return delete_command


def run_command(command, timeout):
    print("Running command")
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return process.wait(timeout), process.stdout.read().decode('utf-8')


def fail(message):
    log.error(message)
    sys.exit(1)


def main():
    args = parse_args()
    cmd = ''
    if args.cmd == 'list':
        cmd = build_datastream_list_command(args)
    elif args.cmd == 'create':
        cmd = build_datastream_create_command(args)
    elif args.cmd == 'delete':
        cmd = build_datastream_delete_command(args)

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
