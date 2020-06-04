import logging
import requests

from typing import Tuple, Union
from testlib import ALT_SSL_CAFILE
from testlib.brooklin.environment import BROOKLIN_PRODUCT_NAME, BROOKLIN_SERVICE_NAME, BrooklinClusterChoice
from testlib.core.teststeps import TestStep
from testlib.core.utils import send_request, retry, OperationFailedError, typename, \
    get_response_json as get_response_json_util
from testlib.range import list_hosts

log = logging.getLogger(__name__)

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
            log.info(f'Retrieving control target hosts for fabric: {fabric} and tag: {tag}')
            control_target.hosts = EkgClient._get_hosts(fabric, tag)
            log.info(f'Retrieved {len(control_target.hosts)} control hosts')

        if not exp_target.hosts:
            fabric, tag = exp_target.fabric, exp_target.tag
            log.info(f'Retrieving experiment target hosts for fabric: {fabric} and tag: {tag}')
            exp_target.hosts = EkgClient._get_hosts(fabric, tag)
            log.info(f'Retrieved {len(exp_target.hosts)} experiment hosts')

        request_body = EkgClient._get_request_body(product, app, control_target, exp_target, start_secs,
                                                   end_secs)

        log.info('Submitting analysis report request to EKG server')
        analysis_id = self._submit_analysis_request(request_body, ssl_cafile)
        log.info(f'Analysis report submitted successfully. Analysis ID: {analysis_id}')

        return analysis_id, self._get_analysis_status(analysis_id, ssl_cafile)

    def _submit_analysis_request(self, request_body, ssl_cafile):
        err, response = self._submit_request(request_body, ssl_cafile)
        if err is not None:
            raise err
        response_json: dict = EkgClient._get_response_json(response)
        analysis_id: str = response_json.get('data', {}).get('id')
        if not analysis_id or not analysis_id.isdigit():
            raise OperationFailedError(f'Received an invalid JSON response from EKG server; response did not '
                                       f'contain analysis ID:\n{response_json}')
        return int(analysis_id)

    @retry(tries=10, delay=20, predicate=lambda result: result[0] is None)
    def _submit_request(self, request_body, ssl_cafile):
        try:
            response = send_request(send_fn=lambda: requests.post(url=self.ANALYSIS_URL, json=request_body,
                                                                  verify=ssl_cafile),
                                    error_message='Failed to submit analysis request to EKG server')
        except OperationFailedError as err:
            log.exception('Submitting analysis report to EKG failed')
            return err, None
        else:
            return None, response

    @retry(tries=16, delay=60, predicate=lambda status: status in {ANALYSIS_PASS, ANALYSIS_FAIL})
    def _get_analysis_status(self, analysis_id, ssl_cafile):
        url = f'{self.ANALYSIS_URL}/{analysis_id}'

        log.info(f'Retrieving analysis report ID: {analysis_id}')
        response = send_request(send_fn=lambda: requests.get(url, verify=ssl_cafile),
                                error_message=f'Failed to retrieve EKG analysis report with ID: {analysis_id}')
        response_json: dict = EkgClient._get_response_json(response)
        status = response_json.get('data', {}).get('attributes', {}).get('status')
        if not status:
            raise OperationFailedError(f'Received response with no analysis status from EKG server:\n{response_json}')
        return status

    @staticmethod
    def _get_request_body(product, app, control_target: EkgTarget, exp_target: EkgTarget, start_secs, end_secs):
        control_targets = EkgClient._get_targets('CONTROL', product, app, control_target, start_secs, end_secs)
        experiment_targets = EkgClient._get_targets('EXPERIMENT', product, app, exp_target, start_secs, end_secs)
        return dict(analysis_type='CANARY', targets=experiment_targets + control_targets)

    @staticmethod
    def _get_hosts(fabric, tag):
        hosts = list_hosts(fabric, tag)
        if not hosts:
            raise OperationFailedError(f'No hosts found for the specified fabric: {fabric} and tag: {tag}')
        return hosts

    @staticmethod
    def _get_targets(type, product, app, target: EkgTarget, start_secs, end_secs):
        return [dict(type=type, fabric=target.fabric, product=product, application=app, instance=target.instance,
                     tag=target.tag, hostname=h, start_timestamp=start_secs, end_timestamp=end_secs)
                for h in target.hosts]

    @staticmethod
    def _get_response_json(response):
        return get_response_json_util(response, error_message='Received an invalid response from EKG server')


class RunEkgAnalysis(TestStep):
    """Test step for running EKG analysis
    """

    def __init__(self, starttime_getter, endtime_getter):
        super().__init__()
        if not starttime_getter or not endtime_getter:
            raise ValueError('At least one of time getter is invalid')

        self.starttime_getter = starttime_getter
        self.endtime_getter = endtime_getter
        self.ekg_client = EkgClient()

    def run_test(self):
        analysis_id, status = self.ekg_client.run_analysis(
            product=BROOKLIN_PRODUCT_NAME, app=BROOKLIN_SERVICE_NAME,
            control_target=EkgTarget(fabric=BrooklinClusterChoice.CONTROL.value.fabric,
                                     tag=BrooklinClusterChoice.CONTROL.value.tag),
            exp_target=EkgTarget(fabric=BrooklinClusterChoice.EXPERIMENT.value.fabric,
                                 tag=BrooklinClusterChoice.EXPERIMENT.value.tag),
            start_secs=self.starttime_getter(), end_secs=self.endtime_getter(), ssl_cafile=ALT_SSL_CAFILE)

        crt_url = f'https://crt.prod.linkedin.com/#/testing/ekg/analyses/{analysis_id}'
        if status != ANALYSIS_PASS:
            raise OperationFailedError(f'EKG analysis did not pass. CRT url: {crt_url}')
        log.info(f'EKG analysis passed. CRT url: {crt_url}')
