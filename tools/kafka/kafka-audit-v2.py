#!/usr/bin/env python3
"""This is a script which hits the Kafka Audit V2 endpoint to obtain completeness counts for a list of topics within
a given time frame."""

import argparse
import logging
import re
import sys
import time
from multiprocessing.pool import ThreadPool as Pool

import requests
import tqdm

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')
log = logging.getLogger()

BASE_URL = 'http://kafka-auditing-reporter.corp-lca1.atd.corp.linkedin.com:8332/kafka-auditing-reporter/v2/api'

TOPICS_API = '/completeness/topics'
COUNTS_API = '/completeness/counts?topic={topicName}&start={startTimeMs}&end={endTimeMs}&version=v2&pipeline=cert'


def parse_args():
    def csv(topics):
        if re.match('^[-_.A-z0-9]+(,[-_A-z0-9]+)*,?$', topics):
            return [i for i in topics.split(',') if i]
        raise argparse.ArgumentError('invalid list of topics')

    parser = argparse.ArgumentParser(description='Get counts of all BMM topics processed by Kafka Audit V2')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--topics', type=csv, help='[Optional] CSV of topics')
    parser.add_argument('--output', help='[Optional] path to store logs and results')
    parser.add_argument('--topicsfile', help='[Optional] path to input topics file, with a single topic on each line')
    parser.add_argument('--startms', '-s', type=int,
                        help='Start time in milliseconds since epoch. --endms is required', required=True)
    parser.add_argument('--endms', '-e', type=int, help='End time in milliseconds since epoch. --startms is required',
                        required=True)
    parser.add_argument('--threshold', '-t', type=int, help='Percentage threshold at which to fail audit', default=3)

    p = parser.parse_args()

    if not p.debug:
        log.setLevel(logging.INFO)

    if p.endms and not p.startms:
        raise argparse.ArgumentError('--endms requires --startms/--starthoursago')

    if p.topicsfile:
        file_topics = get_topic_list_from_file(p.topicsfile)
        if p.topics:
            p.topics = p.topics + file_topics
        else:
            p.topics = file_topics

    return p


def get_topic_list_from_file(topics_file):
    file_topics = []
    with open(topics_file) as file:
        for line in file:
            topic = line.strip()
            if topic[0] != '#':
                file_topics.append(topic)
    return file_topics


def get_audit_counts(topic, start_ms, end_ms):
    url = BASE_URL + COUNTS_API.format(topicName=topic, startTimeMs=start_ms, endTimeMs=end_ms)
    log.debug('Querying counts for topic {0} with URL: "{1}"'.format(topic, url))
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        print("Error in processing topic %s" % topic)
    raise ValueError()


def is_cert_tier(topic_counts):
    if 'cert' in ",".join(topic_counts['totalsPerTier'].keys()):
        return True
    return False


def process(topic, start_ms, end_ms):
    try:
        topic_counts = get_audit_counts(topic, start_ms, end_ms)
        if 'cert' in ",".join(topic_counts['totalsPerTier'].keys()):
            return topic, topic_counts
    except ValueError as e:
        log.error('Unable to get audit counts, error: {}'.format(e), file=sys.stderr)
        return False
    return False


def process_unpack(args):
    return process(*args)


def find_cert_tier_counts(topics, start_ms, end_ms):
    pool = Pool(4)
    topic_counts_map = dict(filter(None, list(
        tqdm.tqdm(pool.imap(process_unpack, [(topic, start_ms, end_ms) for topic in topics]), total=len(topics)))))
    pool.close()
    return topic_counts_map


def get_all_topics():
    r = requests.get(BASE_URL + TOPICS_API)
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
    p = parse_args()

    if p.output:
        sys.stdout = open(p.output, 'w')

    log.info('Checking audit FROM "{0}" TO "{1}"'.format(time.ctime(p.startms / 1000), time.ctime(p.endms / 1000)))
    if not p.topics:
        topics = get_all_topics()
        topic_counts_map = find_cert_tier_counts(topics, p.startms, p.endms)
    else:
        topic_counts_map = find_cert_tier_counts(p.topics, p.startms, p.endms)

    print_summary_table(topic_counts_map)
    print('\nCounts were from beginTimestamp={0}({1}) to endTimestamp={2}({3})'.format(
            p.startms, time.ctime(p.startms / 1000), p.endms, time.ctime(p.endms / 1000)))
    is_pass = aggregate_and_verify_topic_counts(topic_counts_map, p.threshold)
    print('Aggregate audit counting pass threshold {}, passed: {}'.format(p.threshold, is_pass))

    sys.stdout = sys.__stdout__

    if is_pass:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
