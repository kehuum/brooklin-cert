#!/usr/bin/env python3
"""This is a script which uses Kafka admin client to perform Kafka administrative operations such as topic creation,
topic deletion, listing topics in a cluster, etc"""

import argparse
import logging

from testlib.likafka.admin import AdminClient

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger()

CREATE_TOPIC_COMMAND = 'create_topic'
DELETE_TOPIC_COMMAND = 'delete_topic'
LIST_TOPICS_COMMAND = 'list_topics'


def parse_args():
    parser = argparse.ArgumentParser(description='Perform all Kafka Admin Client administrative operations')

    # Create a common parser which can be shared by all sub-commands
    common_parser = argparse.ArgumentParser(add_help=False)

    # Add all the optional arguments for the common parser
    common_parser.add_argument('--debug', action='store_true')

    # Add all the required arguments for the common parser
    required_common = common_parser.add_argument_group('required arguments')
    required_common.add_argument('--bs', dest='bootstrap_servers', required=True, action='append',
                                 help='Bootstrap server to be used to create the Kafka AdminClient. Must be in the'
                                      ' format of HOSTNAME:PORT. This can be repeated to specify multiple bootstrap'
                                      ' servers')
    required_common.add_argument('--cf', dest='cert_file', help='Kafka AdminClient cert file full path', required=True)
    required_common.add_argument('--kf', dest='key_file', help='Kafka AdminClient key file full path', required=True)

    # Choose CRUD command to run with relevant details
    subcommands = parser.add_subparsers(help='Help for supported commands')
    subcommands.dest = 'cmd'
    subcommands.required = True

    def add_subparser(name, help):
        return subcommands.add_parser(name, help=help, parents=[common_parser])

    create_topic_command_parser(add_subparser)
    delete_topic_command_parser(add_subparser)
    list_topics_command_parser(add_subparser)

    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    return args


def create_topic_command_parser(add_parser):
    # All the arguments for the create topic sub-command
    create_command = add_parser(CREATE_TOPIC_COMMAND, help='Create a Kafka topic')

    # Add all the required arguments for the create topic sub-command
    create_command_group = create_command.add_argument_group('required arguments')
    create_command_group.add_argument('--name', '-n', required=True, help='Topic name')

    create_command_optional_group = create_command.add_argument_group('optional arguments')
    create_command_optional_group.add_argument('--partitions', '-p', default=8, type=int,
                                               help='The number of partitions to create the topic with. Defaults to 8')
    create_command_optional_group.add_argument('--rf', dest='replication_factor', default=3, type=int,
                                               help='The replication factor s to create the topic with. Defaults to 3')
    create_command_optional_group.add_argument('--tc', dest='topic_configs', action='append',
                                               help='Topic configs defined as key-value pairs separated by ":"')
    create_command.set_defaults(cmd=CREATE_TOPIC_COMMAND)


def delete_topic_command_parser(add_parser):
    # All the arguments for the delete topic sub-command
    delete_command = add_parser(DELETE_TOPIC_COMMAND, help='Delete a Kafka topic')

    # Add all the required arguments for the delete topic sub-command
    delete_command_group = delete_command.add_argument_group('required arguments')
    delete_command_group.add_argument('--name', '-n', required=True, help='Topic name')
    delete_command.set_defaults(cmd=DELETE_TOPIC_COMMAND)


def list_topics_command_parser(add_parser):
    # All the arguments for the list topic sub-command
    list_command = add_parser(LIST_TOPICS_COMMAND, help='List Kafka topics in a cluster')
    list_command.set_defaults(cmd=LIST_TOPICS_COMMAND)


def create_topic(admin_client, args):
    admin_client.create_topic(args.name, args.partitions, args.replication_factor, args.topic_configs)


def delete_topic(admin_client, args):
    admin_client.delete_topic(args.name)


def list_topics(admin_client, args):
    return admin_client.list_topics()


def main():
    args = parse_args()

    commands = {
        CREATE_TOPIC_COMMAND: create_topic,
        DELETE_TOPIC_COMMAND: delete_topic,
        LIST_TOPICS_COMMAND: list_topics
    }

    admin_client = AdminClient(args.bootstrap_servers, args.cert_file, args.key_file)
    run_command_fn = commands[args.cmd]
    run_command_fn(admin_client, args)


if __name__ == '__main__':
    main()
