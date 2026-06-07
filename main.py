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

producer = Producer(producer_conf)

#Defining a function to create a topic. We are using the AdminClient to create the topic. We are checking if the topic already exists before creating it. If the topic already exists, we will log a message and return. If the topic does not exist, we will create the topic and log a message.

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers":"KAFKA_BROKERS"})

    try:
        metadata = admin_client.list_topics(timeout=10)     #It is a cluster metadata. It return all the information about the cluster in a dictionary format, like topic name, broker id, name, partitions etc.
        if topic_name not in metadata.topic:    # metadata.topic list all the topic name in the cluster
            topic = NewTopic(                   # Here topic is basically a topic config object which will later be used to create an actual topic
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
            
            fs = admin_client.create_topics([topic]) # It creates a future object with a ey value pair. Key is topic name, and value is the state of the operation
            for topic,future in fs.items():
                try:
                    future.result()     # It chesk the result of the current iteam of the fs object. If the topic is created, result = None, which will do nothing and proceed to the next line of code. If it result = None exception = some exception, then it will skip staright to the exception block. The immediate lineswon't be read before the exception block. Hence, returning nothing ensure the success of creating the topic.
                    logger.info(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed creating topic: {e}")
        else:
            logger.info(f"Topic '{topic_name}' already exists.")

    except Exception as e;
        logger.error(f"Erro creating topic: {e}")


#Function which generates sinthetic transactions:
def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userid=f"user_{random.randint(1,100)}",
        amount=round(random.uniform(50000,150000),2),
        transactiontime=int(time.time()),
        merchantId=random.choice(['merchant_1','merchant_2','merchant_3']),
        transactionType=random.choice(['purchase','Refund']),
        location=f'location_{random.randint(1,50)}',
        paymentMethod=random.choice(['credit_card','paypal','bank_transfer']),
        isInternational=random.choice(['True','False']),
        currency=random.choice(['USD','EUR','GBP'])
    )   

#Adding a call back function which will be automatically called after the producer delivered a message. We'll see later in the code in producer.produce() section.Once the message is sent,  producer gets an acknowledgment (or an error) from the broker, it internally calls delivery_report(err, msg) where err — a KafkaError object if delivery failed, or None if it succeeded
#msg — the Message object representing what was sent, which has methods like .key(), .value(), .topic(), .partition(), .offset().

def delivery_report(err,msg):
    if err is not None:
        print(f'Delivery failed for record {msg.key()}')
    else:
        print(f'Record {msg.key} was successfully produced.')


