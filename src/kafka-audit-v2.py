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

BASE_URL = 'http://kafka-auditing-reporter.corp-lca1.atd.corp.linkedin.com:8332' \
           '/kafka-auditing-reporter/v2/api/completeness'

KAFKA_LVA1_CERT_KEY = 'kafka-lva1-cert'
KAFKA_LOR1_CERT_KEY = 'kafka-brooklin-cert'
TOTALS_PER_TIER = 'totalsPerTier'


def parse_args():
    parser = argparse.ArgumentParser(description='Get counts of all BMM topics processed by Kafka Audit V2')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--topics', type=csv, default=[], help='CSV of topics')
    parser.add_argument('--output', help='Path to store logs and results')
    parser.add_argument('--topicsfile', help='Path to input topics file, with a single topic on each line')
    parser.add_argument('--threshold', '-t', type=int, help='Percentage threshold at which to fail audit', default=3)
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
    counts_api = f'/counts?topic={topic}&start={start_ms}&end={end_ms}&version=v2&pipeline=brooklin-cert'
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
    return 'cert' in ",".join(topic_counts[TOTALS_PER_TIER].keys())


def process(topic, start_ms, end_ms):
    try:
        topic_counts = get_audit_counts(topic, start_ms, end_ms)
    except ValueError:
        log.exception('Unable to get audit counts')
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
    format_str = "{0: <50} {1: >20} {2: >20} {3: >16} {4: >3.2f}%"
    header = format_str.replace(': >3.2f', '').format('Topic', KAFKA_LVA1_CERT_KEY, KAFKA_LOR1_CERT_KEY,
                                                      'CountDifference', 'Complete')
    print('\n')
    print(topic_counts)
    print('\n')
    print(header)
    print("=" * len(header))
    for topic in topic_counts:
        try:
            lva1count = topic_counts[topic][TOTALS_PER_TIER][KAFKA_LVA1_CERT_KEY]
        except:
            log.error(f'Counts missing for {KAFKA_LVA1_CERT_KEY} tier for topic: {topic}')
            continue
        try:
            lor1count = topic_counts[topic][TOTALS_PER_TIER][KAFKA_LOR1_CERT_KEY]
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


def aggregate_and_verify_topic_counts(topic_counts, threshold, per_topic_validation):
    total_prod_lva1 = 0
    total_prod_lor1 = 0
    lva1_topic_missing = 0
    lor1_topic_missing = 0
    topics_outside_threshold = 0

    for topic in topic_counts:
        lva1count = 0
        lor1count = 0
        skip_count = False

        try:
            lva1count = topic_counts[topic][TOTALS_PER_TIER][KAFKA_LVA1_CERT_KEY]
        except:
            log.debug(f'Counts missing for {KAFKA_LVA1_CERT_KEY} tier for topic: {topic}')
            lva1_topic_missing = lva1_topic_missing + 1
            skip_count = True

        try:
            lor1count = topic_counts[topic][TOTALS_PER_TIER][KAFKA_LOR1_CERT_KEY]
        except:
            log.debug(f'Counts missing for {KAFKA_LOR1_CERT_KEY} tier for topic: {topic}')
            lor1_topic_missing = lor1_topic_missing + 1
            skip_count = True

        # Only count for topics that have counts for both prod-lva1 and prod-lor1
        if not skip_count:
            total_prod_lva1 += lva1count
            total_prod_lor1 += lor1count

            # Calculate whether the topic's counts are within the threshold
            topic_value = lor1count / max(lva1count, 1) * 100
            outside_threshold = topic_value < 100 - threshold or topic_value > 100 + threshold
            if outside_threshold:
                topics_outside_threshold += 1

    value = total_prod_lor1 / max(total_prod_lva1, 1) * 100
    print(f'total_prod_lor1: {total_prod_lor1}, total_prod_lva1: {total_prod_lva1}, ratio: {value} %')
    if lva1_topic_missing or lor1_topic_missing:
        log.warning(f'Topic missing count: lva1: {lva1_topic_missing}, lor1: {lor1_topic_missing}')
    if topics_outside_threshold:
        log.warning(f'Some topics counts are outside the allowed threshold ({threshold}): {topics_outside_threshold}')

    aggregate_within_threshold = 100 - threshold <= value < 100 + threshold
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
    is_pass = aggregate_and_verify_topic_counts(topic_counts_map, args.threshold, args.pertopicvalidation)
    audit_level = "Per-topic" if args.pertopicvalidation else "Aggregate"
    print(f'{audit_level} audit counting pass threshold: {args.threshold}, passed: {is_pass}')
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
