#!/usr/bin/env python3

import argparse
import logging
import sys

from testlib import ALT_SSL_CAFILE
from testlib.ekg import EkgTarget, EkgClient, ANALYSIS_PASS, ANALYSIS_FAIL
from testlib.core.utils import OperationFailedError


def parse_args():
    parser = argparse.ArgumentParser(
        description='Hit EKG endpoints to generate and retrieve performance analysis reports')
    # optional arguments
    parser.add_argument('-v', '--verbose', required=False, action='store_true', default=False,
                        help='Enable verbose logging (default = False)')
    parser.add_argument('--ca', dest='ssl_cafile', required=False, default=ALT_SSL_CAFILE,
                        help=f'SSL certificate authority file path (default = {ALT_SSL_CAFILE})')
    parser.add_argument('-i', '--instance', required=False, default='i001',
                        help='Application instance (default = i001)')
    parser.add_argument('-p', '--product', required=False, default='brooklin-server',
                        help='Multiproduct name (default = brooklin-server)')
    parser.add_argument('-a', '--app', required=False, default='brooklin-service',
                        help='Application name (default = brooklin-service)')

    # required arguments
    required_arguments_group = parser.add_argument_group('required arguments')
    required_arguments_group.add_argument('--cf', dest='control_fabric', required=True,
                                          help='Fabric to use as control')
    required_arguments_group.add_argument('--ef', dest='exp_fabric', required=True,
                                          help='Fabric to use as experiment')
    required_arguments_group.add_argument('--ct', dest='control_tag', required=True,
                                          help='Product tag to use as control')
    required_arguments_group.add_argument('--et', dest='exp_tag', required=True,
                                          help='Product tag to use as experiment')
    required_arguments_group.add_argument('-s', dest='start_secs', metavar='START_SEC', required=True, type=int,
                                          help='Analysis start time in seconds since epoch')
    required_arguments_group.add_argument('-e', dest='end_secs', metavar='END_SEC', required=True, type=int,
                                          help='Analysis end time in seconds since epoch')
    return parser.parse_args()


def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)


def main():
    args = parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')

    ekg_client = EkgClient()
    try:
        analysis_id, status = ekg_client.run_analysis(product=args.product, app=args.app,
                                                      control_target=EkgTarget(fabric=args.control_fabric,
                                                                               tag=args.control_tag),
                                                      exp_target=EkgTarget(fabric=args.exp_fabric,
                                                                           tag=args.exp_tag),
                                                      start_secs=args.start_secs, end_secs=args.end_secs,
                                                      ssl_cafile=args.ssl_cafile)
    except (ValueError, OperationFailedError) as err:
        fail(f'EKG analysis failed with the following error:\n{err}')
    else:
        crt_url = f'https://crt.prod.linkedin.com/#/testing/ekg/analyses/{analysis_id}'
        print(f'CRT analysis URL: {crt_url}')

        if status == ANALYSIS_PASS:
            print('EKG analysis succeeded')
        elif status == ANALYSIS_FAIL:
            fail(f'EKG analysis failed')
        else:
            fail(f'EKG analysis inconclusive. Analysis status: {status}')


if __name__ == '__main__':
    main()
