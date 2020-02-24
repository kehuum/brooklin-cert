import argparse
import re


def typename(o: object):
    """Gets the typename of Python objects"""
    return type(o).__name__


def csv(tokens):
    """Splits comma-separated strings into tokens"""
    if re.match('^[-_.A-z0-9]+(,[-_A-z0-9]+)*,?$', tokens):
        return [t for t in tokens.split(',') if t]
    raise argparse.ArgumentTypeError('Invalid comma-delimited list; '
                                     'only alphanumeric characters, digits, dashes, dots, and underscores are allowed')


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
