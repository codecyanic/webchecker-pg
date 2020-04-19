#!/usr/bin/env python3

import json
import logging
import yaml

from psycopg2.pool import ThreadedConnectionPool
from kafka import KafkaConsumer

import db
import jobs
from jobqueue import JobQueue

logger = logging.getLogger(__name__)


def json_deserialize(raw):
    try:
        return json.loads(raw)
    except Exception as e:
        logger.exception(e)


def load_config():
    config_paths = [
        '/etc/webchecker/webchecker-pg.yaml',
        'config.yaml',
    ]
    for path in config_paths:
        try:
            with open(path) as f:
                config = yaml.safe_load(f)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(e)

    try:
        return config
    except:
        logger.critical('\nError while loading config file')
        exit(1)


if __name__ == '__main__':
    config = load_config()

    if config.get('debug'):
        jobs.logger.addHandler(logging.StreamHandler())
        jobs.logger.setLevel(logging.DEBUG)

    postgresql = config.get('postgresql', {})
    threads = postgresql.get('threads', 1)
    pg_uri = postgresql.get('uri')

    kafka = config.get('kafka', {})
    topics = kafka.pop('topics', '')
    if isinstance(topics, str):
        topics = (topics,)

    kafka['value_deserializer'] = json_deserialize
    if 'auto_offset_reset' not in kafka:
        kafka['auto_offset_reset'] = 'earliest'

    print('Setting up PostgreSQL...')
    pool = ThreadedConnectionPool(1, threads, pg_uri)
    with db.get_conn(pool) as c:
        db.run_ddl(c)

    print('Connecting to Kafka...')
    consumer = KafkaConsumer(*topics, **kafka)

    jq = JobQueue(threads)
    print('\nWebChecker-pg service is running\n')
    for message in consumer:
        jq.put(jobs.commit_message, (pool, message))
