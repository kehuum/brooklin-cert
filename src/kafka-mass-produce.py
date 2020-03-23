#!/usr/bin/env python3

import os
import argparse
import time
import string
import random

from multiprocessing import Process
from kafka import KafkaProducer
from testlib import DEFAULT_CA_FILE, DEFAULT_SSL_CERTFILE
from testlib.core.utils import rate_limit


def parse_args():
    parser = argparse.ArgumentParser(description="Mass-produce data to Kafka")
    required_args = parser.add_argument_group('required arguments')
    required_args.add_argument('--bs', dest='bootstrap_server', required=True,
                               help='Kafka bootstrap server url and port (SSL)')
    required_args.add_argument('-t', '--topic', dest='topic', required=True,
                               help='Kafka topic to produce to')
    required_args.add_argument('-p', '--np', dest='num_producers', type=int, required=True,
                               help='Number of concurrent producers')

    optional_args = parser.add_argument_group('optional arguments')
    optional_args.add_argument('--nm', type=int, dest='num_messages', required=False, default=1000_000,
                               help='Number of messages to produce (default = 1M)')
    optional_args.add_argument('--ms', type=int, dest='message_size', required=False, default=1000,
                               help='Size of each message produced to Kafka topic (default = 1K)')
    optional_args.add_argument('--an', type=bool, dest='use_alphanum', required=False, default=False,
                               help='Generate messages with readable characters, typically slower (default=False)')
    optional_args.add_argument('--rl', type=int, dest='rate_limit_per_sec', required=False,
                               help='Max number of messages to send per second')
    optional_args.add_argument('-c', '--cert', dest='ssl_certfile', required=False, default=DEFAULT_SSL_CERTFILE,
                               help=f'SSL certificate file path (PEM format) (default = ./{DEFAULT_SSL_CERTFILE})')
    optional_args.add_argument('--ca', dest='ssl_cafile', required=False, default=DEFAULT_CA_FILE,
                               help=f'SSL certificate authority file path (default = {DEFAULT_CA_FILE})')

    args = parser.parse_args()
    if not os.path.isfile(args.ssl_certfile):
        parser.error(f'No SSL certificate file found: {args.ssl_certfile}')
    if not os.path.isfile(args.ssl_cafile):
        parser.error(f'No SSL certificate authority file found: {args.ssl_cafile}')
    return args


def get_current_time():
    return time.strftime("%H:%M:%S", time.localtime())


def os_random_bytes(count):
    return os.urandom(count)


def random_alphanum_bytes(count):
    return str.encode(''.join(random.choices(string.ascii_letters + string.digits, k=count)))


def run_single_producer(bootstrap_server, topic, message_count, message_size, use_alphanum, rate_limit_per_sec,
                        ssl_certfile, ssl_cafile):
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server],
                             acks=0,
                             security_protocol="SSL",
                             ssl_check_hostname=False,
                             ssl_cafile=ssl_cafile,
                             ssl_certfile=ssl_certfile,
                             ssl_keyfile=ssl_certfile)
    # choose function to generate bytes
    generate_random_bytes = random_alphanum_bytes if use_alphanum else os_random_bytes

    producer_send = producer.send
    # decorate producer.send with a rate limiter if a rate limit is specified
    if rate_limit_per_sec:
        producer_send = rate_limit(max_limit=rate_limit_per_sec, seconds=1)(producer_send)

    for i in range(message_count):
        message = generate_random_bytes(message_size)
        producer_send(topic=topic, value=message)
        if i > 0 and i % 1000 == 0:
            print(f"[{os.getpid()}] {get_current_time()} produced {i} messages")


def run_producers(bootstrap_server, topic, num_messages, message_size, num_producers, use_alphanum, rate_limit_per_sec,
                  ssl_certfile, ssl_cafile):
    args = (bootstrap_server, topic, num_messages, message_size, use_alphanum, rate_limit_per_sec, ssl_certfile,
            ssl_cafile)
    producers = [Process(target=run_single_producer, args=args) for _ in range(num_producers)]

    for p in producers:
        p.start()

    for p in producers:
        p.join()


def main():
    args = parse_args()
    run_producers(**vars(args))


if __name__ == '__main__':
    main()
