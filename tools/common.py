import argparse
import re


def csv(tokens):
    if re.match('^[-_.A-z0-9]+(,[-_A-z0-9]+)*,?$', tokens):
        return [t for t in tokens.split(',') if t]
    raise argparse.ArgumentTypeError('invalid comma-delimited list; '
                                     'only alphanumeric characters, digits, dashes, dots, and underscores are allowed')
