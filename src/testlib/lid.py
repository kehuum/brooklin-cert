import json
import logging
import uuid
import requests

from enum import Enum
from typing import NamedTuple
from testlib import DEFAULT_SSL_CAFILE, DEFAULT_SSL_CERTFILE
from testlib.core.utils import retry, send_request, get_response_json as get_response_json_util, OperationFailedError

log = logging.getLogger(__name__)

FabricInfo = NamedTuple('FabricInfo', [('fabric', str), ('environment', str)])


class FabricChoice(Enum):
    """Fabric choices to be used with LidClient"""
    PROD_LVA1 = FabricInfo(fabric='prod-lva1', environment='prod')
    PROD_LOR1 = FabricInfo(fabric='prod-lor1', environment='prod')
    PROD_LTX1 = FabricInfo(fabric='prod-ltx1', environment='prod')
    PROD_LSG1 = FabricInfo(fabric='prod-lsg1', environment='prod')
    EI_LTX1 = FabricInfo(fabric='ei-ltx1', environment='stg')

    @staticmethod
    def from_fabric(fabric):
        fabrics = {c.value.fabric: c for c in FabricChoice}
        choice = fabrics.get(fabric)
        if not choice:
            raise ValueError(f'{fabric} is not supported')
        return choice


class DeploymentRequest(object):
    """Represent a deployment request submitted to LID server"""

    def __init__(self, request: dict):
        self.request = request

    def __str__(self):
        return json.dumps(self.request, indent=4)

    @property
    def state(self):
        return self.request.get('state')

    @property
    def is_successful(self):
        return self.state == 'SUCCEEDED'

    @property
    def is_failed(self):
        return self.state == 'FAILED'


class LidClient(object):
    """Client capable of submitting deployment requests to LID server"""

    def __init__(self, ssl_certfile=DEFAULT_SSL_CERTFILE, ssl_cafile=DEFAULT_SSL_CAFILE):
        self.ssl_certfile = ssl_certfile
        self.ssl_cafile = ssl_cafile

    def restart(self, product, fabric, product_tag, host_concurrency):
        request_id = self._submit_restart_request(product, fabric, product_tag, host_concurrency)
        deployment_request: DeploymentRequest = self._get_deployment_request(request_id, fabric)

        if not deployment_request.is_successful:
            raise OperationFailedError('Expected deployment restart request to be successful. '
                                       f'Deployment request ID: {request_id}, state: {deployment_request.state}, '
                                       f'CRT url: {LidClient._crt_deployment_status_url(request_id, fabric)}')

    def _submit_restart_request(self, product, fabric, product_tag, host_concurrency):
        request_id = str(uuid.uuid4())
        # LID server expects these custom Rest.li headers to be set
        request_headers = {'X-RestLi-Protocol-Version': '2.0.0', 'X-RestLi-Method': 'create'}
        request_body = LidClient._get_restart_request_body(request_id, product, fabric, product_tag, host_concurrency)
        log.info(f'Submitting restart request to LID server. Request ID: {request_id}')
        send_request(send_fn=lambda: requests.post(url=LidClient._lid_deployments_url(fabric), json=request_body,
                                                   headers=request_headers, verify=self.ssl_cafile,
                                                   cert=self.ssl_certfile),
                     error_message='Failed to submit restart request to LID server')
        return request_id

    @retry(tries=20, delay=60 * 3,
           predicate=lambda deployment_request: deployment_request.is_successful or deployment_request.is_failed)
    def _get_deployment_request(self, request_id, fabric):
        url = f'{LidClient._lid_deployments_url(fabric)}/{request_id}'
        log.info(f'Retrieving deployment request info for request ID: {request_id}')
        response = send_request(send_fn=lambda: requests.get(url, verify=self.ssl_cafile),
                                error_message=f'Failed to get deployment request ID: {request_id}')
        json_dict: dict = LidClient._get_response_json(response)
        return DeploymentRequest(json_dict)

    @staticmethod
    def _crt_deployment_status_url(request_id, fabric):
        return f'https://crt.prod.linkedin.com/#/deployment/details?' \
               f'fabric={fabric}&releaseId={request_id}'

    @staticmethod
    def _lid_deployments_url(fabric):
        fabric_choice = FabricChoice.from_fabric(fabric).value
        return f'https://1.lid-server.{fabric_choice.fabric}.atd.{fabric_choice.environment}.linkedin.com:31337' \
               '/lidDeploymentRequests'

    @staticmethod
    def _get_response_json(response):
        return get_response_json_util(response, error_message='Received an invalid response from LID server')

    @staticmethod
    def _get_restart_request_body(request_id, product, fabric, product_tag, host_concurrency):
        return dict(product=f'urn:li:multiProduct:{product}', createdBy=f'urn:li:fabric:{fabric}',
                    fabric=f'urn:li:fabric:{fabric}', productTag=product_tag, id=request_id,
                    action={"com.linkedin.deployment.ControlAction": dict(type="RESTART")},
                    options=dict(host_concurrency=host_concurrency / 100))
