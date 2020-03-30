#!/usr/bin/env python3
"""This is a script which hits the Kafka Audit V2 endpoint to obtain completeness counts for a list of topics within
a given time frame."""

import argparse
import logging
import sys
import time
import requests
import tqdm

from multiprocessing.pool import ThreadPool as Pool
from testlib.core.utils import csv

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger()

BASE_URL = 'http://kafka-auditing-reporter.corp-lca1.atd.corp.linkedin.com:8332' \
           '/kafka-auditing-reporter/v2/api/completeness'


def parse_args():
    parser = argparse.ArgumentParser(description='Get counts of all BMM topics processed by Kafka Audit V2')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--topics', type=csv, default=[], help='CSV of topics')
    parser.add_argument('--output', help='Path to store logs and results')
    parser.add_argument('--topicsfile', help='Path to input topics file, with a single topic on each line')
    parser.add_argument('--threshold', '-t', type=int, help='Percentage threshold at which to fail audit', default=3)

    required_arguments_group = parser.add_argument_group('required arguments')
    required_arguments_group.add_argument('--startms', '-s', type=int, required=True,
                                          help='Start time in milliseconds since epoch. --endms is required')
    required_arguments_group.add_argument('--endms', '-e', type=int, required=True,
                                          help='End time in milliseconds since epoch. --startms is required')
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)

    if args.endms and not args.startms:
        parser.error('--endms requires --startms/--starthoursago')

    if args.topicsfile:
        args.topics += get_topic_list_from_file(args.topicsfile)

    return args


def get_topic_list_from_file(topics_file):
    return [line.strip() for line in open(topics_file) if not line.startswith('#')]


def get_audit_counts(topic, start_ms, end_ms):
    counts_api = f'/counts?topic={topic}&start={start_ms}&end={end_ms}&version=v2&pipeline=cert'
    url = BASE_URL + counts_api
    log.debug('Querying counts for topic {0} with URL: "{1}"'.format(topic, url))
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        return r.json()
    print('Error in processing topic %s' % topic)
    raise ValueError(f'Error in processing topic {topic}, status code: {r.status_code}')


def is_cert_tier(topic_counts):
    return 'cert' in ",".join(topic_counts['totalsPerTier'].keys())


def process(topic, start_ms, end_ms):
    try:
        topic_counts = get_audit_counts(topic, start_ms, end_ms)
    except ValueError as e:
        log.error('Unable to get audit counts, error: {}'.format(e))
        return False
    else:
        return (topic, topic_counts) if is_cert_tier(topic_counts) else False


def find_cert_tier_counts(topics, start_ms, end_ms):
    def process_unpack(args):
        return process(*args)

    with Pool(4) as pool:
        topic_counts_map = dict(filter(None, list(
            tqdm.tqdm(pool.imap(process_unpack, [(topic, start_ms, end_ms) for topic in topics]), total=len(topics)))))
        return topic_counts_map


def get_all_topics():
    topics_api = '/topics'
    url = BASE_URL + topics_api
    r = requests.get(url)
    return r.json()


def print_summary_table(topic_counts):
    total_prod_lva1 = 0
    total_prod_lor1 = 0
    format_str = "{0: <50} {1: >16} {2: >16} {3: >16} {4: >3.2f}%"
    header = format_str.replace(': >3.2f', '').format('Topic', 'kafka-lva1-cert', 'kafka-lor1-cert', 'CountDifference',
                                                      'Complete')
    print('\n')
    print(topic_counts)
    print('\n')
    print(header)
    print("=" * len(header))
    for topic in topic_counts:
        try:
            lva1count = topic_counts[topic]['totalsPerTier']['kafka-lva1-cert']
        except:
            log.error('Counts missing for kafka-lva1-cert tier for topic: {0}'.format(topic))
            continue
        try:
            lor1count = topic_counts[topic]['totalsPerTier']['kafka-lor1-cert']
        except:
            log.error('Counts missing for kafka-lor1-cert tier for topic: {0}'.format(topic))
            continue
        total_prod_lva1 += lva1count
        total_prod_lor1 += lor1count
        print(format_str.format(topic, lva1count, lor1count, lor1count - lva1count,
                                (100.0 * lor1count / (lva1count or 1))))

    footer = format_str.format('Total [{0} topics]:'.format(len(topic_counts)), total_prod_lva1, total_prod_lor1,
                               total_prod_lor1 - total_prod_lva1, (100.0 * total_prod_lor1 / (total_prod_lva1 or 1)))
    print("=" * len(footer))
    print(footer)


def aggregate_and_verify_topic_counts(topic_counts, threshold):
    total_prod_lva1 = 0
    total_prod_lor1 = 0
    lva1_topic_missing = 0
    lor1_topic_missing = 0

    for topic in topic_counts:
        lva1count = 0
        lor1count = 0
        lva1_topic_missing = 0
        lor1_topic_missing = 0

        try:
            lva1count = topic_counts[topic]['totalsPerTier']['kafka-lva1-cert']
        except:
            log.debug('Counts missing for kafka-lva1-cert tier for topic: {0}'.format(topic))
            lva1_topic_missing = lva1_topic_missing + 1

        try:
            lor1count = topic_counts[topic]['totalsPerTier']['kafka-lor1-cert']
        except:
            log.debug('Counts missing for kafka-lor1-cert tier for topic: {0}'.format(topic))
            lor1_topic_missing = lor1_topic_missing + 1

        total_prod_lva1 += lva1count
        total_prod_lor1 += lor1count

    value = float(total_prod_lor1 / float(total_prod_lva1 or 1) * 100)
    print('total_prod_lor1: %s, total_prod_lva1: %s, ratio: %s %%' % (total_prod_lor1, total_prod_lva1, value))
    if lva1_topic_missing or lor1_topic_missing:
        log.info('Topic missing count: lva1: %s, lor1: %s' % (lva1_topic_missing, lor1_topic_missing))
    return float(100 - threshold) <= value < float(100 + threshold)


def main():
    args = parse_args()

    if args.output:
        sys.stdout = open(args.output, 'w')

    log.info('Checking audit FROM "{0}" TO "{1}"'.format(time.ctime(args.startms / 1000), time.ctime(args.endms / 1000)))
    if not args.topics:
        topics = get_all_topics()
        topic_counts_map = find_cert_tier_counts(topics, args.startms, args.endms)
    else:
        topic_counts_map = find_cert_tier_counts(args.topics, args.startms, args.endms)

    print_summary_table(topic_counts_map)
    print('\nCounts were from beginTimestamp={0}({1}) to endTimestamp={2}({3})'.format(
            args.startms, time.ctime(args.startms / 1000), args.endms, time.ctime(args.endms / 1000)))
    is_pass = aggregate_and_verify_topic_counts(topic_counts_map, args.threshold)
    print('Aggregate audit counting pass threshold {}, passed: {}'.format(args.threshold, is_pass))

    sys.stdout = sys.__stdout__

    sys.exit(0 if is_pass else 1)


if __name__ == '__main__':
    main()
