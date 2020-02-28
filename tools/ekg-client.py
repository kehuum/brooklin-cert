#!/usr/bin/env python3

import argparse
import sys
import requests
import logging

from common import typename, list_hosts, OperationFailedError, retry, send_request, DEFAULT_CA_FILE

ANALYSIS_PASS = 'PASS'
ANALYSIS_FAIL = 'FAIL'


class EkgTarget(object):
    def __init__(self, fabric, tag, hosts=None, instance="i001"):
        self.fabric = fabric
        self.tag = tag
        self.hosts = hosts or []
        self.instance = instance

    def __str__(self):
        return f'{typename(self)}: ' \
               f'fabric: {self.fabric}, ' \
               f'tag: {self.tag}, ' \
               f'hosts={len(self.hosts)}, ' \
               f'instance={self.instance})'


class EkgClient(object):
    ANALYSIS_URL = 'https://mega.tools.corp.linkedin.com/api/v1/analyses'

    def run_analysis(self, product, app, control_target: EkgTarget, exp_target: EkgTarget, start_secs, end_secs,
                     ssl_cafile):
        if end_secs <= start_secs:
            raise ValueError(f'Invalid start time: {start_secs} and end times {end_secs}; '
                             f'end time must be greater than start time')

        if not control_target.hosts:
            fabric, tag = control_target.fabric, control_target.tag
            logging.info(f'Retrieving control target hosts for fabric: {fabric} and tag: {tag}')
            control_target.hosts = EkgClient.get_hosts(fabric, tag)
            logging.info(f'Retrieved {len(control_target.hosts)} control hosts')

        if not exp_target.hosts:
            fabric, tag = exp_target.fabric, exp_target.tag
            logging.info(f'Retrieving experiment target hosts for fabric: {fabric} and tag: {tag}')
            exp_target.hosts = EkgClient.get_hosts(fabric, tag)
            logging.info(f'Retrieved {len(exp_target.hosts)} experiment hosts')

        request_body = EkgClient.get_request_body(product, app, control_target, exp_target, start_secs,
                                                  end_secs)

        logging.info('Submitting analysis report request to EKG server')
        analysis_id = self.submit_analysis_request(request_body, ssl_cafile)
        logging.info(f'Analysis report submitted successfully. Analysis ID: {analysis_id}')

        return analysis_id, self.get_analysis_status(analysis_id, ssl_cafile)

    def submit_analysis_request(self, request_body, ssl_cafile):
        response = send_request(send_fn=lambda: requests.post(url=self.ANALYSIS_URL, json=request_body,
                                                              verify=ssl_cafile),
                                error_message='Failed to submit analysis request to EKG server')

        response_json: dict = EkgClient.get_response_json(response)

        analysis_id: str = response_json.get('data', {}).get('id')
        if not analysis_id or not analysis_id.isdigit():
            raise OperationFailedError(f'Received an invalid JSON response from EKG server; response did not '
                                       f'contain analysis id:\n{response_json}')
        return int(analysis_id)

    @retry(tries=10, delay=60, backoff=2, predicate=lambda status: status in {ANALYSIS_PASS, ANALYSIS_FAIL})
    def get_analysis_status(self, analysis_id, ssl_cafile):
        url = f'{self.ANALYSIS_URL}/{analysis_id}'

        logging.info(f'Retrieving analysis report ID: {analysis_id}')
        response = send_request(send_fn=lambda: requests.get(url, verify=ssl_cafile),
                                error_message=f'Failed to retrieve EKG analysis report with id: {analysis_id}')
        response_json: dict = EkgClient.get_response_json(response)
        status = response_json.get('data', {}).get('attributes', {}).get('status')
        if not status:
            raise OperationFailedError(f'Received response with no analysis status from EKG server:\n{response_json}')
        return status

    @staticmethod
    def get_request_body(product, app, control_target: EkgTarget, exp_target: EkgTarget, start_secs, end_secs):
        control_targets = EkgClient.get_targets('CONTROL', product, app, control_target, start_secs, end_secs)
        experiment_targets = EkgClient.get_targets('EXPERIMENT', product, app, exp_target, start_secs, end_secs)
        return dict(analysis_type='CANARY', targets=experiment_targets + control_targets)

    @staticmethod
    def get_hosts(fabric, tag):
        hosts = list_hosts(fabric, tag)
        if not hosts:
            raise OperationFailedError(f'No hosts found for the specified fabric: {fabric} and tag: {tag}')
        return hosts

    @staticmethod
    def get_targets(type, product, app, target: EkgTarget, start_secs, end_secs):
        return [dict(type=type, fabric=target.fabric, product=product, application=app, instance=target.instance,
                     tag=target.tag, hostname=h, start_timestamp=start_secs, end_timestamp=end_secs)
                for h in target.hosts]

    @staticmethod
    def get_response_json(response):
        try:
            return response.json()
        except ValueError as err:
            raise OperationFailedError(f'Received an invalid response from EKG server; response contained invalid or '
                                       f'empty json content:\n{response}', err)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Hit EKG endpoints to generate and retrieve performance analysis reports')
    # optional arguments
    parser.add_argument('-v', '--verbose', required=False, action='store_true', default=False,
                        help='Enable verbose logging (default = False)')
    parser.add_argument('--ca', dest='ssl_cafile', required=False, default=DEFAULT_CA_FILE,
                        help=f'SSL certificate authority file path (default = {DEFAULT_CA_FILE})')
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
        logging.basicConfig(level=logging.DEBUG)

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
