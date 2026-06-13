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

# Putting the streaming dataframe object in a variable.This just sets up the configuration, no data flow starts yet. Its like setting up the hose pipe in the garden, connected to the water tap. The tap isn't open yet.
# Data will strats loading once there is .start()
# .readStream tells spark that there will be continuous data flow. the flow will be kafka. the next line connects with the broker server. The following line tell swhich topic should it listen to. and then it tells from where it should read data(offset). Again this is
# this is just a configuration. The real data flow will start later.
kafka_stream = spark.readstream \
    .format("kafka") \
    .option("kafka.bootstap.servers", KAFKA_BROKERS) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffset", "earliest") \
    .load()


# Kafka messages are raw bytes on the wire. This code is converting those bytes into a proper structured DataFrame with named columns.
# .selectExpr() is used to write SQL expression as string.
# Kafka gives us a DataFrame with fixed columns — key, value, topic, partition, offset, timestamp. The value column is raw bytes. This line casts it to a readable string like '{"userId": "123", "amount": 50.0, ...}'. We're essentially just making it human readable text first. All these, "userId": "123", are you raw bytes. selectExpr("CAST(value AS STRING)") is making it plain string first.
# Now, value column is sitting there just as simple text. .select(from_json(col("value"), transaction_schema) code will parse those text through a spark stuct(which is still key value paired like {"userId": "123", "amount": 50.0, ...}), but now it is distinguishable. userId is string, amoun is double etc. They are not just plain text anymore. from_json() parse that text and recognize the key-value structure inside the plain text. Then it applied transaction_schema to give each value a proper type (userId → String, amount → Double)
# Now that we have spark struct, .select(.*) will explode that key value pairs into proper row column dataframe that we will be able to use later for example to filter group by etc.
transaction_df = kafka_stream.selectExpr("CAST(value as STRING)") \
    .select(from_json(col("value"),tansaction_schema).alias("data")) \
    .select("data.*")
    