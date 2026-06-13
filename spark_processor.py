from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, DoubleType, LongType

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
SOURCE_TOPIC = "financial_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALIES_TOPIC = "transactions_anomalies"
CHECKPOINT_DIR = "/mnt/spark-checkpoints"
STATES_DIR = "/mnt/spark-state"

# Creating spark session, wired up with Kafka support, checkpointing, and tuned partition counts for a streaming pipeline that reads financial transactions.
# .appName names the application. This label appears in the Spark UI and logs so you can identify this job.
# Defauld partition is 200. We're doing 5 here.
# Checkpoints store progress (offsets read, watermarks) so Spark can recover from crashes without reprocessing everything. 
spark = SparkSession.builder \
    .appName("FinancialTransactionProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)\
    .config("spark.sql.shuffle.partitions", 5) \
    .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR) \
    .getOrCreate()

# Setting log level. It tells Spark to only show log messages that are warnings or above (WARN, ERROR).
spark.sparkContext.setLogLevel("WARN")

# Setting the schema
transaction_schema = StructType([
    StructField("transactionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("merchantId", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transactionTime", LongType(), True),
    StructField("transactionType", StringType(), True),
    StructField("location", StringType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("isInternational", StringType(), True),
    StructField("currency", StringType(), True)
    
])