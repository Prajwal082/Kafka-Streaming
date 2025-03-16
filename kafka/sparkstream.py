import os 
import logging

from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *

from typing import *

from dotenv import load_dotenv

class Structuredstream():

    def __init__(self,bootstrap_servers:str,topic_name:str,startingOffsets:str) -> None:

        self.logger = logging.getLogger(__class__.__name__)
        
        load_dotenv()

        self.__initVars()
        
        self.spark = self.__initSparksession()

        self.bootstrap_servers = bootstrap_servers
        
        self.topic_name = topic_name
        
        self.startingOffsets = startingOffsets


    def __initVars(self):

        self.__account_key = os.environ['STORAGE_ACCOUNT_KEY']
        self.__client_id = os.environ['CLIENT_ID']
        self.__tenant_id = os.environ['TENANT_ID']
        self.__client_secret = os.environ['CLIENT_SECRET']
        self.__storage_account_name = os.environ['STORAGE_ACCOUNT_NAME']
        self.__container_name = os.environ['CONTAINER_NAME']
        self.__snowflake_url = os.environ['SNOWFLAKE_URL']
        self.__snowflake_user = os.environ['SNOWFLAKE_USER']
        self.__snowflake_password = os.environ['SNOWFLAKE_PASSWORD']
        self.__snowflake_role = os.environ['SNOWFLAKE_ROLE']
        self.__snowflake_db = os.environ['SNOWFLAKE_DB']
        self.__snowflake_schema = os.environ['SNOWFLAKE_SCHEMA']
        self.__snowflake_warehouse = os.environ['SNOWFLAKE_WAREHOUSE']



    @property
    def kafkaOptions(self) -> Dict:
        return {
            'kafka.bootstrap.servers' : self.bootstrap_servers,
            'subscribe' : self.topic_name,
            'startingOffsets' : self.startingOffsets
        }

    def __initSparksession(self):
        return (
            SparkSession
                .builder
                .appName('Kafka Streaming data using Redpanda')
                .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4')
                .config('spark.jars.packages','org.apache.hadoop:hadoop-azure:3.4.1')
                .config('spark.jars.packages','org.apache.hadoop:hadoop-azure-datalake:3.4.1')
                .config('spark.jars.packages','org.apache.hadoop:hadoop-common:3.4.1')
                .config('spark.jars.packages','net.snowflake:spark-snowflake_2.12:3.1.1')
                .config('spark.jars.packages','net.snowflake:snowflake-jdbc:3.23.0')
                .config("spark.hadoop.fs.azure.account.key.poctrials.dfs.core.windows.net",self.__account_key)
                .getOrCreate()
        )


    def applyTransformations(self,df:DataFrame) ->DataFrame:

        order_schema = StructType([
            StructField("OrderId", StringType(), False),
            StructField("OrderDate", DateType(), False),
            StructField("CustomerID", StringType(), False),
            StructField("CustomerFirstName", StringType(), False),
            StructField("CustomerLastName", StringType(), False),
            StructField("CustomerPhone", StringType(), False),
            StructField("CustomerEmail", StringType(), False),

            StructField("Location", StructType([
                StructField("City", StringType(), False),
                StructField("Pincode", StringType(), False),
                StructField("Region", StringType(), False)
            ]), False),

            StructField("Product", StructType([
                StructField("ProductID", IntegerType(), False),
                StructField("ProductName", StringType(), False),
                StructField("Category", StringType(), False),
                StructField("Brand", StringType(), False),
                StructField("UnitPrice", DoubleType(), False),
                StructField("Discount", DoubleType(), False)
            ]), False),

            StructField("TotalUnits", DoubleType(), False),
            StructField("POS", StringType(), False),
            StructField("OrderStatus", StringType(), False),
            StructField("TotalAmount", DoubleType(), False),

            StructField("Payment", StructType([
                StructField("PaymentMethod", StringType(), False),
                StructField("PaymentStatus", StringType(), False),
                StructField("TransactionID", StringType(), False)
            ]), False),

            StructField("ShippingDetails", StructType([
                StructField("ShippingAddress", StructType([
                    StructField("Shipping_Street", StringType(), False),
                    StructField("Shipping_City", StringType(), False),
                    StructField("Shipping_State", StringType(), False),
                    StructField("Shipping_ZipCode", IntegerType(), False),
                    StructField("Shipping_Country", StringType(), False)
                ]), False),
                StructField("ShippingMethod", StringType(), False),
                StructField("EstimatedDelivery", DateType(), False)
            ]), False),

            StructField("Timestamp", TimestampType(), False)
        ])

        # By default kafka will send value in bytes we will convert to string 
        falattened_json_df = (df.withColumn("value", col('value').cast(StringType())).select('value')
                    .withColumn('values_json',from_json(col('value'),order_schema))
                    .selectExpr('values_json.*')
                    .drop('value')
                )

        falattened_json_df = falattened_json_df.select(
            col('OrderId'),
            col('OrderDate'),
            col('CustomerID'),
            col('CustomerFirstName'),
            col('CustomerLastName'),
            col('CustomerPhone'),
            col('CustomerEmail'),
            col('Location.*'),
            col('Product.*'),
            col('TotalUnits'),
            col('POS'),
            col('OrderStatus'),
            col('TotalAmount'),
            col('Payment.*'),
            col('ShippingDetails.ShippingAddress.*'),
            col('ShippingDetails.ShippingMethod'),
            col('ShippingDetails.EstimatedDelivery'),
            col('Timestamp')
            )

        return falattened_json_df

    def launch(self):

        #Set config params described above
        self.spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
        self.spark.conf.set("spark.hadoop.fs.azure.account.auth.type."+self.__storage_account_name +".dfs.core.windows.net", "OAuth")
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth.provider.type."+self.__storage_account_name +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.id."+self.__storage_account_name +".dfs.core.windows.net", self.__client_id)
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.secret."+self.__storage_account_name +".dfs.core.windows.net", self.__client_secret)
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.endpoint."+self.__storage_account_name +".dfs.core.windows.net", "https://login.microsoftonline.com/"+self.__tenant_id+"/oauth2/token")

        path = "abfss://"+self.__container_name+"@"+self.__storage_account_name +".dfs.core.windows.net/"
        
        kafka_df = (
            self.spark
                .readStream
                .format('kafka')
                .options(**self.kafkaOptions)
                .load()
        )

        flattened_df = self.applyTransformations(kafka_df)

        
        def process_batch(batchDF:DataFrame,batchID:int):
            self.logger.info(f'Now processing batch {batchID}')
        
            (
                batchDF
                .write
                .format('snowflake')
                .options(**sfOptions) 
                .option('dbtable','orders')
                .mode('append')
                .save()
            )

            batchDF.show()

        sfOptions = {
                        "sfURL": self.__snowflake_url,
                        "sfDatabase": self.__snowflake_db,
                        "sfSchema": self.__snowflake_schema,
                        "sfWarehouse": self.__snowflake_warehouse,
                        "sfRole": self.__snowflake_role,
                        "sfuser": self.__snowflake_user,
                        "sfpassword": self.__snowflake_password,
                        "autopushdown": "on"  # Enables Snowflake pushdown optimization
                    }


        (
            flattened_df
            .writeStream
            .outputMode('append')
            .foreachBatch(process_batch)
            .trigger(once=True)
            .option('checkpointLocation',path +'checkpoint/')
            .start()
            .awaitTermination()
        )


if __name__ =='__main__':

    bootstrap_servers = 'redpanda-0:9092'
    topic = 'orders_topic'
    startingOffsets = 'earliest'

    stream = Structuredstream(bootstrap_servers,topic,startingOffsets)

    stream.launch()

# docker exec -it sparkstreaming-spark-master-1 spark-submit --master spark://172.20.0.4:7077 --deploy-mode client --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.hadoop:hadoop-azure:3.4.1,org.apache.hadoop:hadoop-common:3.4.1,org.apache.hadoop:hadoop-azure-datalake:3.4.1  jobs/sparkstream.py