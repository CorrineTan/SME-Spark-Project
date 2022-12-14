#! /usr/bin/env python3

import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

bootstrap = ['b-1.corrine-kafka.9wq81s.c5.kafka.us-east-1.amazonaws.com:9092','b-3.corrine-kafka.9wq81s.c5.kafka.us-east-1.amazonaws.com:9092','b-2.corrine-kafka.9wq81s.c5.kafka.us-east-1.amazonaws.com:9092']
topic = 'CorrineNewTopic'

def gnerator():
    id = 0
    line = {}
    while True:
        line['id'] = id
        line['date'] = '2022-03-15'
        line['name'] = 'Corrine'
        line['sales'] = 12345
        yield line
        id = id + 1

def sendToKafka():
    producer = KafkaProducer(bootstrap_servers=bootstrap)

    for line in gnerator():
        data = json.dumps(line).encode('utf-8')

        # Asynchronous by default
        future = producer.send(topic, data)

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            # Decide what to do if produce request failed...
            pass
        time.sleep(0.1)

sendToKafka()