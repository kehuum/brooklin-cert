import requests

from testlib.core.utils import OperationFailedError


def list_hosts(fabric, tag):
    """Retrieves the list of hostnames that match the specified fabric and tag"""

    url = f'http://range.corp.linkedin.com/range/list?%{fabric}.tag_hosts:{tag}'
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as err:
        raise OperationFailedError('Encountered an error issuing a request to range server', err)
    else:
        status_code = response.status_code
        if status_code != requests.codes.ok:
            raise OperationFailedError(f'Received a response with unsuccessful status code from range server: '
                                       f'{status_code}')
        return response.text.splitlines()