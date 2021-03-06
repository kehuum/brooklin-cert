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
from testlib.core.utils import csv, retry

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger()

BASE_URL = 'http://pipeline-completeness-monitor-cert.prod.linkedin.com' \
           '/pipeline-completeness-monitor/api/'

KAFKA_LVA1_CERT_KEY = 'lva1-cert'
KAFKA_LOR1_CERT_KEY = 'lor1-brooklin-cert'
TIER_COUNTS_KEY = 'tierCounts'
TOTAL_COUNT_KEY = "totalCount"
BMM_ALERT_NAME = "bmm-release-cert"
STREAM_NAMES_KEY = "streamNames"


def parse_args():
    parser = argparse.ArgumentParser(description='Get counts of all BMM topics processed by Kafka Audit V2')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--topics', type=csv, default=[], help='CSV of topics')
    parser.add_argument('--output', help='Path to store logs and results')
    parser.add_argument('--topicsfile', help='Path to input topics file, with a single topic on each line')
    parser.add_argument('--lowthreshold', '--lt', type=float, help='Percentage low threshold at which to fail audit',
                        default=1.0)
    parser.add_argument('--highthreshold', '--ht', type=float, help='Percentage high threshold at which to fail audit',
                        default=3.0)
    parser.add_argument('--pertopicvalidation', action='store_true')

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
    counts_api = f'report/counts?alertName={BMM_ALERT_NAME}&streamName={topic}' \
        f'&beginTimestamp={start_ms}&endTimestamp={end_ms}'
    url = BASE_URL + counts_api
    log.debug(f'Querying counts for topic {topic} with URL: "{url}"')
    # Send the request with a connect timeout of 10 seconds and read timeout of 10 seconds to prevent waiting too
    # long to receive a response
    r = requests.get(url, timeout=(10, 10))
    if r.status_code == requests.codes.ok:
        return r.json()
    print(f'Error in processing topic {topic}')
    raise ValueError(f'Error in processing topic {topic}, status code: {r.status_code}')


def is_cert_tier(topic_counts):
    return 'cert' in ",".join(topic_counts[TIER_COUNTS_KEY].keys())


def process(topic, start_ms, end_ms):
    try:
        topic_counts = get_audit_counts(topic, start_ms, end_ms)
    except ValueError:
        log.exception('Unable to get audit counts')
        return False
    else:
        return (topic, topic_counts)


def find_cert_tier_counts(topics, start_ms, end_ms):
    def process_unpack(args):
        return process(*args)

    with Pool(4) as pool:
        topic_counts_map = dict(filter(None, list(
            tqdm.tqdm(pool.imap(process_unpack, [(topic, start_ms, end_ms) for topic in topics]), total=len(topics)))))
        return topic_counts_map


def get_all_topics():
    topics_api = f'alerts/{BMM_ALERT_NAME}'
    url = BASE_URL + topics_api
    r = requests.get(url)
    return r.json()[STREAM_NAMES_KEY]


def print_summary_table(topic_counts):
    total_prod_lva1 = 0
    total_prod_lor1 = 0
    format_str = "{0: <50} {1: >20} {2: >20} {3: >16} {4: >3.2f}%"
    header = format_str.replace(': >3.2f', '').format('Topic', KAFKA_LVA1_CERT_KEY, KAFKA_LOR1_CERT_KEY,
                                                      'CountDifference', 'Complete')
    print('\n')
    print(header)
    print("=" * len(header))
    for topic in topic_counts:
        try:
            lva1count = topic_counts[topic][TIER_COUNTS_KEY][0][TOTAL_COUNT_KEY]
        except:
            log.error(f'Counts missing for {KAFKA_LVA1_CERT_KEY} tier for topic: {topic}')
            continue
        try:
            lor1count = topic_counts[topic][TIER_COUNTS_KEY][1][TOTAL_COUNT_KEY]
        except:
            log.error(f'Counts missing for {KAFKA_LOR1_CERT_KEY} tier for topic: {topic}')
            continue
        total_prod_lva1 += lva1count
        total_prod_lor1 += lor1count
        print(format_str.format(topic, lva1count, lor1count, lor1count - lva1count,
                                (100.0 * lor1count / (lva1count or 1))))

    footer = format_str.format('Total [{0} topics]:'.format(len(topic_counts)), total_prod_lva1, total_prod_lor1,
                               total_prod_lor1 - total_prod_lva1, (100.0 * total_prod_lor1 / (total_prod_lva1 or 1)))
    print("=" * len(footer))
    print(footer)


def aggregate_and_verify_topic_counts(topic_counts, low_threshold, high_threshold, per_topic_validation):
    total_prod_lva1 = 0.0
    total_prod_lor1 = 0.0
    lva1_topic_missing = 0
    lor1_topic_missing = 0
    topics_outside_threshold = 0

    for topic in topic_counts:
        lva1count = 0
        lor1count = 0
        skip_count = False

        try:
            lva1count = topic_counts[topic][TIER_COUNTS_KEY][0][TOTAL_COUNT_KEY]
        except:
            log.debug(f'Counts missing for {KAFKA_LVA1_CERT_KEY} tier for topic: {topic}')
            lva1_topic_missing = lva1_topic_missing + 1
            skip_count = True

        try:
            lor1count = topic_counts[topic][TIER_COUNTS_KEY][1][TOTAL_COUNT_KEY]
        except:
            log.debug(f'Counts missing for {KAFKA_LOR1_CERT_KEY} tier for topic: {topic}')
            lor1_topic_missing = lor1_topic_missing + 1
            skip_count = True

        # Only count for topics that have counts for both prod-lva1 and prod-lor1
        if not skip_count:
            total_prod_lva1 += lva1count
            total_prod_lor1 += lor1count

            # Calculate whether the topic's counts are within the threshold
            topic_value = lor1count / max(lva1count, 1.0) * 100.0
            outside_threshold = topic_value < 100.0 - low_threshold or topic_value > 100.0 + high_threshold
            if outside_threshold:
                topics_outside_threshold += 1

    value = total_prod_lor1 / max(total_prod_lva1, 1.0) * 100.0
    print(f'total_prod_lor1: {total_prod_lor1}, total_prod_lva1: {total_prod_lva1}, ratio: {value} %')
    if lva1_topic_missing or lor1_topic_missing:
        log.warning(f'Topic missing count: lva1: {lva1_topic_missing}, lor1: {lor1_topic_missing}')
    if topics_outside_threshold:
        log.warning(f'Some topics counts are outside the allowed low threshold ({low_threshold}) or high threshold '
                    f'({high_threshold}): {topics_outside_threshold}')

    aggregate_within_threshold = 100.0 - low_threshold <= value < 100.0 + high_threshold
    if per_topic_validation:
        log.info(f'Topics outside threshold: {topics_outside_threshold}, aggregate within threshold: '
                 f'{aggregate_within_threshold}')
        return topics_outside_threshold == 0 and aggregate_within_threshold
    return aggregate_within_threshold


@retry(tries=16, delay=2 * 60)
def run_audit(args):
    startms_ctime = time.ctime(args.startms / 1000)
    endms_ctime = time.ctime(args.endms / 1000)
    log.info(f'Checking audit FROM "{startms_ctime}" TO "{endms_ctime}"')
    if not args.topics:
        topics = get_all_topics()
        topic_counts_map = find_cert_tier_counts(topics, args.startms, args.endms)
    else:
        topic_counts_map = find_cert_tier_counts(args.topics, args.startms, args.endms)

    print_summary_table(topic_counts_map)
    print(f'\nCounts were from beginTimestamp={args.startms}({startms_ctime}) to '
          f'endTimestamp={args.endms}({endms_ctime})')
    is_pass = aggregate_and_verify_topic_counts(topic_counts_map, args.lowthreshold, args.highthreshold,
                                                args.pertopicvalidation)
    audit_level = "Per-topic" if args.pertopicvalidation else "Aggregate"
    print(f'{audit_level} audit counting pass low threshold: {args.lowthreshold}, high threshold: '
          f'{args.highthreshold}, passed: {is_pass}')
    return is_pass


def main():
    args = parse_args()

    if args.output:
        sys.stdout = open(args.output, 'w')

    is_pass = run_audit(args)

    sys.stdout = sys.__stdout__

    sys.exit(0 if is_pass else 1)


if __name__ == '__main__':
    main()
