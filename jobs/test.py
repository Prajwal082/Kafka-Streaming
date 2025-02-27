from pyspark.sql import SparkSession


spark = (
    SparkSession
    .builder
    .appName('Kafka Streaming data using Redpanda')
    .master("sc://172.19.0.2:7077")
    .config('spark.jars.packages','org.apache.iceberg:iceberg-core:1.7.1')
    .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4')
    .getOrCreate()
)

print(spark.version)

kafka_options = {
    'kafka.bootstrap.servers' : 'redpanda-0:9092',
    'subscribe' : 'orders_topic',
    'startingOffsets' : 'earliest'
}

# df = (
#     spark
#     .readStream
#     .format('kafka')
#     .options(**kafka_options)
#     .load()
# )

# query = df.writeStream.format('console').start()

# query.awaitTermination()

# docker exec -it redpanda-quickstart-spark-master-1 spark-submit --master spark://172.20.0.2:7077 jobs/test.py