#!/usr/bin/env python3
"""This is a script which uses Kafka admin client to perform Kafka administrative operations such as topic creation,
topic deletion, listing topics in a cluster, etc"""

import argparse
import logging
import os

from testlib import DEFAULT_SSL_CERTFILE, DEFAULT_SSL_CAFILE
from testlib.likafka.admin import AdminClient

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logging.getLogger('kafka').setLevel(logging.WARN)
log = logging.getLogger()

CREATE_TOPIC_COMMAND = 'create_topic'
DELETE_TOPIC_COMMAND = 'delete_topic'
LIST_TOPICS_COMMAND = 'list_topics'
GET_PARTITION_COUNTS_COMMAND = 'get_partition_counts'


class ArgumentParser(object):
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Perform all Kafka Admin Client administrative operations')

        # Create a common parser which can be shared by all sub-commands
        self.common_parser = argparse.ArgumentParser(add_help=False)

        # Add all the optional arguments for the common parser
        self.common_parser.add_argument('--debug', action='store_true')
        self.common_parser.add_argument('--cert', dest='ssl_certfile', default=DEFAULT_SSL_CERTFILE,
                                        help=f'SSL certificate file path (PEM format) '
                                             f'(default = {DEFAULT_SSL_CERTFILE})')
        self.common_parser.add_argument('--ca', dest='ssl_cafile', default=DEFAULT_SSL_CAFILE,
                                        help=f'SSL certificate authority file path (default = {DEFAULT_SSL_CAFILE})')

        # Add all the required arguments for the common parser
        required_common = self.common_parser.add_argument_group('required arguments')
        required_common.add_argument('--bs', dest='bootstrap_servers', required=True, action='append',
                                     help='Bootstrap server to be used to create the Kafka AdminClient. Must be in the'
                                          ' format of HOSTNAME:PORT. This can be repeated to specify multiple bootstrap'
                                          ' servers')

        # Choose CRUD command to run with relevant details
        self.subcommands = self.parser.add_subparsers(help='Help for supported commands')
        self.subcommands.dest = 'cmd'
        self.subcommands.required = True

        self._create_topic_command_parser()
        self._delete_topic_command_parser()
        self._list_topics_command_parser()
        self._get_partition_counts_command_parser()

    def _add_subparser(self, name, help):
        return self.subcommands.add_parser(name, help=help, parents=[self.common_parser])

    def _create_topic_command_parser(self):
        # All the arguments for the create topic sub-command
        create_command = self._add_subparser(CREATE_TOPIC_COMMAND, help='Create a Kafka topic')

        # Add all the required arguments for the create topic sub-command
        create_command_group = create_command.add_argument_group('required arguments')
        create_command_group.add_argument('--name', '-n', required=True, help='Topic name')

        create_command_optional_group = create_command.add_argument_group('optional arguments')
        create_command_optional_group.add_argument('--partitions', '-p', default=8, type=int,
                                                   help='The number of partitions to create the topic with '
                                                        '(default = 8)')
        create_command_optional_group.add_argument('--rf', dest='replication_factor', default=3, type=int,
                                                   help='The replication factor to create the topic with (default = 3)')
        create_command_optional_group.add_argument('--tc', dest='topic_configs', action='append',
                                                   help='Topic configs defined as key-value pairs separated by ":"')
        create_command.set_defaults(cmd=CREATE_TOPIC_COMMAND)

    def _delete_topic_command_parser(self):
        # All the arguments for the delete topic sub-command
        delete_command = self._add_subparser(DELETE_TOPIC_COMMAND, help='Delete a Kafka topic')

        # Add all the required arguments for the delete topic sub-command
        delete_command_group = delete_command.add_argument_group('required arguments')
        delete_command_group.add_argument('--name', '-n', required=True, help='Topic name')
        delete_command.set_defaults(cmd=DELETE_TOPIC_COMMAND)

    def _get_partition_counts_command_parser(self):
        # All the arguments for the get partition counts sub-command
        get_partition_counts_command = self._add_subparser(GET_PARTITION_COUNTS_COMMAND,
                                                           help='Get partition counts for a topic regex')

        # Add all the required arguments for the get partition counts sub-command
        get_partition_counts_command_group = get_partition_counts_command.add_argument_group('required arguments')
        get_partition_counts_command_group.add_argument('--tr', dest='topic_regex', required=True, help='Topic regex')
        get_partition_counts_command.set_defaults(cmd=GET_PARTITION_COUNTS_COMMAND)

    def _list_topics_command_parser(self):
        # All the arguments for the list topic sub-command
        list_command = self._add_subparser(LIST_TOPICS_COMMAND, help='List Kafka topics in a cluster')
        list_command.set_defaults(cmd=LIST_TOPICS_COMMAND)

    def parse_args(self):
        args = self.parser.parse_args()

        if not os.path.isfile(args.ssl_certfile):
            self.parser.error(f'No SSL certificate file found: {args.ssl_certfile}')
        if not os.path.isfile(args.ssl_cafile):
            self.parser.error(f'No SSL certificate authority file found: {args.ssl_cafile}')

        if args.debug:
            log.setLevel(logging.DEBUG)
        return args


def create_topic(admin_client, args):
    admin_client.create_topic(args.name, args.partitions, args.replication_factor, args.topic_configs)


def delete_topic(admin_client, args):
    admin_client.delete_topic(args.name)


def list_topics(admin_client, args):
    print("\n".join(admin_client.list_topics()))


def get_partition_counts(admin_client, args):
    print(f'Topic regex passed: {args.topic_regex}')
    partition_count = admin_client.get_partition_counts(args.topic_regex.encode().decode('unicode_escape'))
    print(f'Partition counts for topic_regex \"{args.topic_regex}\" is {partition_count}')


def main():
    args = ArgumentParser().parse_args()

    commands = {
        CREATE_TOPIC_COMMAND: create_topic,
        DELETE_TOPIC_COMMAND: delete_topic,
        LIST_TOPICS_COMMAND: list_topics,
        GET_PARTITION_COUNTS_COMMAND: get_partition_counts
    }

    admin_client = AdminClient(args.bootstrap_servers, args.ssl_certfile)
    run_command_fn = commands[args.cmd]
    run_command_fn(admin_client, args)


if __name__ == '__main__':
    main()
