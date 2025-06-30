from kafka import KafkaProducer
import json 
import logging

def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8') 

    )

def send_to_kafka(producer, topic, data, batch = False):
    try:
        if batch:
            for item in data:
                producer.send(topic, value=item)
        else:
            producer.send(topic, value=data)
        producer.flush()
    except Exception as e:
        logging.error(f"errow when sending data {e}",exc_info=True)
        return False

def close_producer(producer):
    try:
        producer.flush()
        producer.close  
        logging.info("producer closed")
    except Exception as e:  
        logging.error(f"errow when closing producer {e}",exc_info=True)
        return False

def setup_logger(name="streamer_logger"):
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)