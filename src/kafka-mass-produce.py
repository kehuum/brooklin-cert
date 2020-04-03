#!/usr/bin/env python3

import os
import argparse
import time
import string
import random

from multiprocessing import Process
from kafka import KafkaProducer
from testlib import DEFAULT_SSL_CAFILE, DEFAULT_SSL_CERTFILE
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

    group = optional_args.add_mutually_exclusive_group(required=True)
    group.add_argument('--random', action='store_true', dest='use_random',
                       help='Generate message with random bytes from /dev/urandom (default)')
    group.add_argument('--alphanum', action='store_true', dest='use_alphanum',
                       help='Generate messages with random readable characters, typically slower')
    group.add_argument('--text', action='store_true', dest='use_text',
                       help='Generate messages with data chunked from a text file')

    optional_args.add_argument('--file', dest='text_file', required=False, default='./data/pride-and-prejudice.txt',
                               help='Input text file to use for generating messages, requires --text '
                                    '(default = ./data/pride-and-prejudice.txt)')
    optional_args.add_argument('--rl', type=int, dest='rate_limit_per_sec', required=False,
                               help='Max number of messages to send per second')
    optional_args.add_argument('-c', '--cert', dest='ssl_certfile', required=False, default=DEFAULT_SSL_CERTFILE,
                               help=f'SSL certificate file path (PEM format) (default = ./{DEFAULT_SSL_CERTFILE})')
    optional_args.add_argument('--ca', dest='ssl_cafile', required=False, default=DEFAULT_SSL_CAFILE,
                               help=f'SSL certificate authority file path (default = {DEFAULT_SSL_CAFILE})')

    args = parser.parse_args()
    if args.use_text and not os.path.isfile(args.text_file):
        parser.error(f'Text file not found: {args.text_file}')
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


def get_bytes_generator(use_random, use_alphanum, use_text, text_file):
    if use_random:
        return os_random_bytes

    if use_alphanum:
        return random_alphanum_bytes

    start = 0
    text = open(text_file, encoding='utf-8-sig').read()

    def text_bytes(count):
        nonlocal start
        result = text[start: start + count]
        if len(result) < count:  # wrap around
            remainder = count - len(result)
            start = remainder
            result += text[:remainder]
        else:
            start += count
        return result.encode('utf-8-sig')

    return text_bytes


def run_single_producer(bootstrap_server, topic, num_messages, message_size, use_random, use_alphanum, use_text,
                        text_file, rate_limit_per_sec, ssl_certfile, ssl_cafile):

    producer = KafkaProducer(bootstrap_servers=[bootstrap_server],
                             acks=0,
                             security_protocol="SSL",
                             ssl_check_hostname=False,
                             ssl_cafile=ssl_cafile,
                             ssl_certfile=ssl_certfile,
                             ssl_keyfile=ssl_certfile)

    # choose function to generate bytes
    generate_bytes = get_bytes_generator(use_random, use_alphanum, use_text, text_file)

    producer_send = producer.send
    # decorate producer.send with a rate limiter if a rate limit is specified
    if rate_limit_per_sec:
        producer_send = rate_limit(max_limit=rate_limit_per_sec, seconds=1)(producer_send)

    for i in range(num_messages):
        message = generate_bytes(message_size)
        producer_send(topic=topic, value=message)
        if i > 0 and i % 1000 == 0:
            print(f"[{os.getpid()}] {get_current_time()} produced {i} messages")


def run_producers(**kwargs):
    producers = [Process(target=run_single_producer, kwargs=kwargs) for _ in range(kwargs.pop('num_producers'))]

    for p in producers:
        p.start()

    for p in producers:
        p.join()


def main():
    args = parse_args()
    run_producers(**vars(args))


if __name__ == '__main__':
    main()
