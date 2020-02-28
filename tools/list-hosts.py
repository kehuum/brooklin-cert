#!/usr/bin/env python3

import argparse
import sys

from common import OperationFailedError, list_hosts


def parse_args():
    parser = argparse.ArgumentParser(description='Retrieve hostnames that meet certain criteria (e.g. fabric)')
    required_arguments_group = parser.add_argument_group('required arguments')
    required_arguments_group.add_argument('-f', '--fabric', required=True, help='Fabric to use')
    required_arguments_group.add_argument('-t', '--tag', required=True, help='Product tag to use')
    return parser.parse_args()


def main():
    args = parse_args()
    fabric, tag = args.fabric, args.tag
    try:
        hosts = list_hosts(fabric, tag)
    except OperationFailedError as err:
        print(f'Listing hosts failed with error:\n{err}', file=sys.stderr)
        sys.exit(1)
    else:
        if hosts:
            print("\n".join(hosts))
        else:
            print(f'No hosts found for fabric {fabric} and tag {tag}', file=sys.stderr)


if __name__ == '__main__':
    main()
