import random
import requests

from testlib.core.utils import send_request


def list_hosts(fabric, tag):
    """Retrieves the list of hostnames that match the specified fabric and tag"""

    url = f'http://range.corp.linkedin.com/range/list?%{fabric}.tag_hosts:{tag}'
    response = send_request(send_fn=lambda: requests.get(url),
                            error_message='Failed to retrieve host list from Range server')
    return response.text.splitlines()


def get_random_host(fabric, tag):
    """Returns a randomly selected host from all the hosts that match the specified fabric and tag"""

    hosts = list_hosts(fabric, tag)
    return random.choice(hosts)
