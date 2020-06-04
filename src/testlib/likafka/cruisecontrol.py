import logging
import re
import requests

from testlib.core.utils import send_request, retry

log = logging.getLogger(__name__)


class CruiseControlClient(object):
    """Class which performs Kafka cluster management using Kafka Cruise Control to do actions such as preferred
    leader election etc."""

    REBALANCE_COMMAND = 'rebalance'
    USER_TASK_ID_HEADER = 'User-Task-ID'
    EXPECTED_RESPONSE_CODES = {requests.codes.ok, requests.codes.accepted, requests.codes.request_uri_too_large}

    def __init__(self, cc_endpoint):
        if not cc_endpoint:
            raise ValueError(f'Invalid cruise control endpoint passed: {cc_endpoint}')

        self.cc_endpoint = cc_endpoint + self.REBALANCE_COMMAND

    def perform_preferred_leader_election(self):
        is_final, user_task_id = self.get_ple_result()
        if not is_final:
            retry_get_ple_result = retry(tries=12, delay=60, predicate=lambda result: result[0])(self.get_ple_result)
            is_final, _ = retry_get_ple_result(headers={self.USER_TASK_ID_HEADER: user_task_id})
        return is_final

    def get_ple_result(self, headers=None):
        params = (
            ('goals', 'PreferredLeaderElectionGoal'),
            ('dryrun', 'false'),
            ('concurrent_leader_movements', '500'),
            ('reason', 'DATAPIPES-18111'),
        )
        response = send_request(send_fn=lambda: requests.post(url=self.cc_endpoint, params=params, headers=headers),
                                error_message=f'Failed to call PLE on cruise control endpoint {self.cc_endpoint}',
                                allowed_status_codes=self.EXPECTED_RESPONSE_CODES)
        log.info(f'Response status: {response.status_code}, response received: {response.text}')
        is_final = CruiseControlClient.is_response_final(response)
        return is_final, response.headers.get(self.USER_TASK_ID_HEADER)

    @staticmethod
    def is_progress_key_absent(response: requests.Response):
        try:
            # Try to guess whether the JSON response is final
            return 'progress' not in response.json().keys()
        except ValueError:
            # We have a non-JSON (probably plain text) response,
            # and a non-202 status code.
            #
            # This response may not be final, but we have no
            # way of doing further guessing, so warn the humans
            # as best as we can, then presume finality.
            cc_version = response.headers.get('Cruise-Control-Version')
            if cc_version is not None:
                log.warning(f'json=False received from cruise-control version ({cc_version}) '
                            'that does not support 202 response codes. '
                            'Please upgrade cruise-control to >=2.0.61, '
                            'Returning a potentially non-final response.')
            elif response.status_code == requests.codes.request_uri_too_large:
                # No cc_version in the response headers
                # cruise-control won't return version information if
                # servlet receives too-large-URI request
                pass
            else:
                log.warning('Unable to determine cruise-control version. '
                            'Returning a potentially non-final response.')
            return True

    @staticmethod
    def integer_cc_version(x):
        return [int(elem) for elem in x.split('.')]

    @staticmethod
    def is_response_final(response: requests.Response):
        if response.status_code == requests.codes.accepted:
            # We're talking to a version of cruise-control that supports
            # 202: accepted, and we know that this response is not final.
            return False

        # Guess about whether this version of cruise-control supports 202
        if "Cruise-Control-Version" in response.headers:
            # define a regex for extracting only leading digits and periods from the Cruise-Control-Version
            non_decimal = re.compile(r'[^\d.]+')
            cc_version = CruiseControlClient. \
                integer_cc_version(non_decimal.sub('', response.headers["Cruise-Control-Version"]))
            if cc_version >= [2, 0, 61]:
                # 202 is supported and was not returned; response final
                return True

        # 202 is not supported and was not returned; guess further
        # or
        # Probably we're getting a response (like a 414) before cruise-control
        # can decorate the headers with version information
        return CruiseControlClient.is_progress_key_absent(response)
