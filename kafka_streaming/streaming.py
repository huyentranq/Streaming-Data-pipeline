from datetime import datetime
import pandas as pd
import time
from kafka_streaming.utils.kafka_func import create_producer, send_to_kafka, close_producer, setup_logger
from kafka_streaming.utils.constants import PIZZA_SALES

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'pizza_sales'
# KAFKA_PARTITION = 10
# REPLICATE_FACTOR = 1

def load_data(file_path=PIZZA_SALES):
    try:
        df = pd.read_csv(file_path)

        #convert dataframe to dictionary     
        pz_sales = df.to_dict(orient='records')
        print(f"📦 Streaming {len(pz_sales)} sales data records to Kafka topic: {KAFKA_TOPIC}")
        print(f"✅ Sample record: {pz_sales[0]}")
        return pz_sales
    except Exception as e:
        logger = setup_logger()
        logger.error(f"errow when loading data {e}",exc_info=True)
        raise

class SalesStreamer:
    def __init__(self,topic=KAFKA_TOPIC):
        self.topic = topic
        self.producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
        self.data = load_data()
        self.current_index = 0
        self.logger = setup_logger()

    def send_batch(self, batch_size):
        try:
            end_index = min(self.current_index + batch_size, len(self.data))
            if self.current_index >= len(self.data):
                raise StopIteration("No more data to stream")
            
            batch = self.data[self.current_index:end_index]
            send_to_kafka(self.producer, self.topic, batch, batch=True)
            print(f"✅ Sent {len(batch)} sales_data records at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.current_index = end_index
        except Exception as e:
            self.logger.error(f"errow when sending data {e}",exc_info=True)
            raise

    def start_streaming(self, batch_size, interval):
        print("starting sales data streaming...| Batch: {batch_size} | Interval: {interval}s")
        try:
            while True:
                self.send_batch(batch_size)
                time.sleep(interval)
        except StopIteration as e:
            self.logger.info(f"Streaming completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except KeyboardInterrupt:
            print("stop by user")
        finally:
            close_producer(self.producer)

#run 
if __name__ == "__main__":
    streamer = SalesStreamer()
    streamer.start_streaming(batch_size=5000, interval=1)














