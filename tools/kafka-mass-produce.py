#!/usr/bin/env python3

import os
import argparse
import string
import random

from multiprocessing import Process
from kafka import KafkaProducer


def parse_args():
    default_ca_file = '/etc/riddler/ca-bundle.crt'
    default_cert_file = 'identity.pem'

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
    optional_args.add_argument('-c', '--cert', dest='ssl_certfile', required=False, default=default_cert_file,
                               help=f'SSL certificate file path (PEM format) (default = ./{default_cert_file})')
    optional_args.add_argument('--ca', dest='ssl_cafile', required=False, default=default_ca_file,
                               help=f'SSL certificate authority file path (PEM format) (default = {default_ca_file})')

    args = parser.parse_args()
    if not os.path.isfile(args.ssl_certfile):
        parser.error(f'No SSL certificate file found: {args.ssl_certfile}')
    if not os.path.isfile(args.ssl_cafile):
        parser.error(f'No SSL certificate authority file found: {args.ssl_cafile}')
    return args


def run_single_producer(bootstrap_server, topic, message_count, message_size, ssl_certfile, ssl_cafile):
    producer = KafkaProducer(bootstrap_servers=[bootstrap_server],
                             acks=0,
                             security_protocol="SSL",
                             ssl_check_hostname=False,
                             ssl_cafile=ssl_cafile,
                             ssl_certfile=ssl_certfile,
                             ssl_keyfile=ssl_certfile)
    for i in range(message_count):
        message = ''.join(random.choices(string.ascii_uppercase + string.digits, k=message_size))
        producer.send(topic=topic, value=str.encode(message))
        if i > 0 and i % 1000 == 0:
            print(f"[{os.getpid()}] produced {i} messages")


def run_producers(bootstrap_server, topic, num_messages, message_size, num_producers, ssl_certfile, ssl_cafile):
    args = (bootstrap_server, topic, num_messages, message_size, ssl_certfile, ssl_cafile)
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
