import argparse
import logging
import sys

from testlib import DEFAULT_SSL_CERTFILE, DEFAULT_SSL_CAFILE
from testlib.core.utils import OperationFailedError
from testlib.lid import LidClient

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger()


def percentage(s: str):
    if s.endswith('%'):
        s = s[:-1]
        if s.isdigit():
            i = int(s)
            if 0 < i <= 100:
                return i
    raise argparse.ArgumentTypeError(f'Invalid percentage: {s}')


def parse_args():
    parser = argparse.ArgumentParser(description='Restart a deployed multiproduct through LID server')

    required_args_group = parser.add_argument_group('required arguments')
    required_args_group.add_argument('-p', '--product', required=True, help='Product name')
    required_args_group.add_argument('-f', '--fabric', required=True, help='Fabric')
    required_args_group.add_argument('--pt', dest='product_tag', required=True, help='Product tag')

    # optional arguments
    parser.add_argument('--hc', dest='host_concurrency', type=percentage, default='10%',
                        help='Host concurrency percentage (default = 10%%)')
    parser.add_argument('--cert', dest='ssl_certfile', default=DEFAULT_SSL_CERTFILE,
                        help=f'SSL certificate file path (PEM format) (default = {DEFAULT_SSL_CERTFILE})')
    parser.add_argument('--ca', dest='ssl_cafile', default=DEFAULT_SSL_CAFILE,
                        help=f'SSL certificate authority file path (default = {DEFAULT_SSL_CAFILE})')

    return parser.parse_args()


def main():
    args = parse_args()
    lid_client = LidClient(ssl_cafile=args.ssl_cafile, ssl_certfile=args.ssl_certfile)
    try:
        lid_client.restart(product=args.product, fabric=args.fabric, product_tag=args.product_tag,
                           host_concurrency=args.host_concurrency)
    except OperationFailedError as err:
        log.error(f'Restarting product failed with error: {err.message}')
        sys.exit(1)
    else:
        log.info(f'Restarting product succeeded. Product: {args.product}, fabric: {args.fabric}, '
                     f'product-tag: {args.product_tag}, host-concurrency: {args.host_concurrency}%')


if __name__ == '__main__':
    main()
