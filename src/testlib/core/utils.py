import argparse
import re
import time
import math
import requests

from typing import Callable, Any


class OperationFailedError(Exception):
    def __init__(self, message, cause=None):
        self._message = message
        self._cause = cause

    @property
    def message(self):
        return self._message

    @property
    def cause(self):
        return self._cause

    def __str__(self):
        s = f'Message: {self.message}'
        if self.cause:
            s += f'\nCause: {typename(self.cause)}: {self.cause}'
        return s


def csv(tokens):
    """Splits comma-separated strings into tokens"""
    if re.match('^[-_.A-z0-9]+(,[-_A-z0-9]+)*,?$', tokens):
        return [t for t in tokens.split(',') if t]
    raise argparse.ArgumentTypeError('Invalid comma-delimited list; '
                                     'only alphanumeric characters, digits, dashes, dots, and underscores are allowed')


def retry(tries, delay=3, backoff=2, predicate: Callable[[Any], bool] = lambda x: x):
    """Retries a function or method until predicate returns a truthful value.
    In all cases, the resulting function returns the return value of the decorated function
    and propagates the exceptions it raises.

    Args:
        - tries:        the number of times to try calling the decorated function, must be at least 0
        - delay:        sets the initial delay in seconds, must be greater than 0
        - backoff:      sets the factor by which the delay should lengthen after each failure, must be
                        greater than 1 or else it isn't really a backoff
        - predicate:    a predicate that is applied to the return value of the decorated function to
                        determine if it succeeded or not
    """

    if backoff <= 1:
        raise ValueError("backoff must be greater than 1")

    tries = math.floor(tries)
    if tries < 0:
        raise ValueError("tries must be 0 or greater")

    if delay <= 0:
        raise ValueError("delay must be greater than 0")

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            _tries, _delay = tries, delay  # make mutable

            while _tries > 0:
                result = f(*args, **kwargs)
                if predicate(result):
                    return result  # function succeeded
                _tries -= 1  # consume an attempt
                if _tries > 0:
                    time.sleep(_delay)  # wait...
                    _delay *= backoff  # make future wait longer

        return f_retry  # true decorator -> decorated function

    return deco_retry  # @retry(arg[, ...]) -> true decorator


def send_request(send_fn: Callable[[], requests.Response], error_message: str) -> requests.Response:
    """Invokes the provided send_fn, returns its response if successful, or throws if it fails

    Args:
        - send_fn:          a function that expects no parameters and returns a requests.Response object
        - error_message:    an error message to set in the raised exception if send_fn raises
                            requests.exceptions.RequestException
    Exceptions:
        - OperationFailedError:
                            raised if send_fn raises requests.exceptions.RequestException or if the response
                            code is not 200 OK

    """
    try:
        response = send_fn()
    except requests.exceptions.RequestException as err:
        raise OperationFailedError(error_message, err)
    else:
        status_code = response.status_code
        if status_code != requests.codes.ok:
            raise OperationFailedError(f'Received non-success response code: {status_code}\n'
                                       f'{error_message}')
        return response


def get_response_json(response, error_message):
    try:
        return response.json()
    except ValueError as err:
        raise OperationFailedError(f'{error_message}; '
                                   f'response contained invalid or empty json content:\n{response}', err)


def typename(o: object):
    """Gets the typename of Python objects"""
    return type(o).__name__