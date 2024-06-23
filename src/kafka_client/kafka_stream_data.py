from src.constants import (
    URL_API,
    endpoints,
    PATH_LAST_PROCESSED,
    API,
    DB_FIELDS,
    yaml_data,
)

from .transformations import extract_data

import yaml
import kafka.errors
import json
import datetime
import requests
from kafka import KafkaProducer
from typing import List
import logging

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO, force=True)




# def dict_from_yaml(yaml_file):
#     with open(yaml_file, "r") as file:
#         return yaml.load(file, Loader=yaml.Loader)
    

def get_data():
    config = yaml_data
    rs = extract_data(config)
    data = rs.run()

    return data

def create_kafka_producer():
    """
    Creates the Kafka producer object
    """
    try:
        producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    except kafka.errors.NoBrokersAvailable:
        logging.info(
            "We assume that we are running locally, so we use localhost instead of kafka and the external "
            "port 9094"
        )
        producer = KafkaProducer(bootstrap_servers=["localhost:9094"])

    return producer


def stream():
    """
    Writes the API data to respective eKafka topics
    """

    for key in DB_FIELDS:
        producer = create_kafka_producer()
        data = get_data()
        results = data.get(key)
        df = results.T.to_dict().values()

        for data in df:
            producer.send(key, json.dumps(data).encode("utf-8"))


if __name__ == "__main__":
    stream()
