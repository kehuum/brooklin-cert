#!/usr/bin/env python3

import argparse
import sys
import requests

from common import typename, OperationFailedError


def parse_args():
    parser = argparse.ArgumentParser(description='Retrieve hostnames that meet certain criteria (e.g. fabric)')
    required_arguments_group = parser.add_argument_group('required arguments')
    required_arguments_group.add_argument('-f', '--fabric', required=True, help='Fabric to use')
    required_arguments_group.add_argument('-t', '--tag', required=True, help='Product tag to use')
    return parser.parse_args()


def list_hosts(fabric, tag):
    url = f'http://range.corp.linkedin.com/range/list?%{fabric}.tag_hosts:{tag}'
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as err:
        raise OperationFailedError('Encountered an error issuing a request to range server', err)
    else:
        status_code = response.status_code
        if status_code != 200:
            raise OperationFailedError(f'Received a response with unsuccessful status code from range server: '
                                       f'{status_code}')
        return response.text.splitlines()


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
