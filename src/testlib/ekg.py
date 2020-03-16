import logging

import requests

from testlib.range import list_hosts
from testlib.core.teststeps import RunPythonCommand
from testlib.core.utils import send_request, retry, OperationFailedError, typename, \
    get_response_json as get_response_json_util

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
                                       f'contain analysis ID:\n{response_json}')
        return int(analysis_id)

    @retry(tries=10, delay=60, backoff=2, predicate=lambda status: status in {ANALYSIS_PASS, ANALYSIS_FAIL})
    def get_analysis_status(self, analysis_id, ssl_cafile):
        url = f'{self.ANALYSIS_URL}/{analysis_id}'

        logging.info(f'Retrieving analysis report ID: {analysis_id}')
        response = send_request(send_fn=lambda: requests.get(url, verify=ssl_cafile),
                                error_message=f'Failed to retrieve EKG analysis report with ID: {analysis_id}')
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
        return get_response_json_util(response, error_message='Received an invalid response from EKG server')


class RunEkgAnalysis(RunPythonCommand):
    """Test step for running EKG analysis
    """

    def __init__(self, starttime_getter, endtime_getter):
        super().__init__()
        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of time getter is invalid')

        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter

    @property
    def main_command(self):
        return 'ekg-client.py ' \
               '--cf prod-lor1 --ct cert.control ' \
               '--ef prod-lor1 --et cert.candidate ' \
               f'-s {self.starttime_getter()} ' \
               f'-e {self.endtime_getter()}'
