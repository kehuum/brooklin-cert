#!/usr/bin/env python3
"""This is a script which uses Cruise Control to perform Kafka administrative operations such as Preferred leader
election, etc"""
import argparse
import logging

from testlib.likafka.cruisecontrol import CruiseControlClient

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger()

PLE_COMMAND = 'ple'


class ArgumentParser(object):
    """Command line arguments parser

    Encapsulates the logic for creating a command line parser with subparsers
    for all the various verbs. Exposes a method, parse_args(), for performing
    argument parsing and validation.

    """

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Perform Kafka Cruise Control administrative operations')

        # Create a common parser which can be shared by all sub-commands
        self.common_parser = argparse.ArgumentParser(add_help=False)

        # Add all the optional arguments for the common parser
        self.common_parser.add_argument('--debug', action='store_true')

        # Add all the required arguments for the common parser
        required_common = self.common_parser.add_argument_group('required arguments')
        required_common.add_argument('--cce', dest='cc_endpoint', required=True, help='Cruise control endpoint to use')

        # Choose cruise-control command to run with relevant details
        self.subcommands = self.parser.add_subparsers(help='Help for supported commands')
        self.subcommands.dest = 'cmd'
        self.subcommands.required = True

        self.ple_command_parser()

    def parse_args(self):
        args = self.parser.parse_args()

        if args.debug:
            log.setLevel(logging.DEBUG)

        return args

    def add_subparser(self, name, help):
        return self.subcommands.add_parser(name, help=help, parents=[self.common_parser])

    def ple_command_parser(self):
        # All the arguments for the create topic sub-command
        ple_command = self.add_subparser(PLE_COMMAND,
                                         help='Perform Preferred Leader Election (PLE) on the Kafka cluster')
        ple_command.set_defaults(cmd=PLE_COMMAND)


def perform_ple(args):
    ple = CruiseControlClient(cc_endpoint=args.cc_endpoint)
    ple.perform_preferred_leader_election()


def main():
    # parse command line arguments
    parser = ArgumentParser()
    args = parser.parse_args()

    commands = {
        PLE_COMMAND: perform_ple
    }

    run_command_fn = commands[args.cmd]
    run_command_fn(args)


if __name__ == '__main__':
    main()
