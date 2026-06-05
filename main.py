import json
import random
import threading
import time
import uuid
from importlib.metadata import metadata
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

##Now we have 3 brokers. 3 brokers will be used to have actual data in it. To store the data, we need to have topic; each topic
##will have 5 partitions and replication factor of 3. Replication factor of 3 means that each partition will be replicated in all 
## 3 brokers. So if one broker goes down, we will still have the data in other 2 brokers. This is how we achieve high availability and fault tolerance in Kafka. 
## We will create a topic named 'financial_transactions' to store the financial transactions data.
KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "financial_transactions"


## We are using logging to log the information and errors. We are setting the logging level to INFO, so that we can see the info messages in the console.
## We are also creating a logger object to log the messages. We will use this logger object to log the messages in the code.
logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger(__name__)


#setting up producer configuration. We are using gzip compression to compress the messages before sending to the broker. This will help us to reduce the network bandwidth and improve the performance of the producer. We are also setting the acks to 1, which means that the producer will wait for the acknowledgment from the leader broker before considering the message as sent. This will provide better latency but less durability as if the leader fails immediately after acknowledging the record but before the followers have replicated it, the record will be lost.
producer_conf = {
    'bootstrap_servers': KAFKA_BROKERS, # list of brokers produer will connect to
    'queue_buffering_max_messages': 10000,# maximum number of messages in the queue before producer freezes and waits for the messages to be sent to the broker
    'queue_buffering_max_kbytes': 512000,# maximum number of kilobytes in the queue before producer freezes and waits for the messages to be sent to the broker
    'batch_num_messages': 1000,# maximum number of messages in a batch before sending to the broker
    'linger_ms': 10,# time to wait before sending a batch of messages to the broker, even if the batch is not full
    'acks': 1,# number of acknowledgments the producer requires the leader to have received before considering a request complete. 1 means that the leader will write the record to its local log but will respond without awaiting full acknowledgment from all followers. This option provides better latency but less durability as if the leader fails immediately after acknowledging the record but before the followers have replicated it, the record will be lost.
    'compression_type': 'gzip'
}

