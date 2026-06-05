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