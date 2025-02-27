import os 

from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *

from typing import *

from dotenv import load_dotenv

class Structuredstream():

    def __init__(self,bootstrap_servers:str,topic_name:str,startingOffsets:str) -> None:
        
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
                .config('spark.jars.packages','org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0')
                .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4')
                .config('spark.jars.packages','org.apache.hadoop:hadoop-azure:3.4.1')
                .config('spark.jars.packages','org.apache.hadoop:hadoop-azure-datalake:3.4.1')
                .config('spark.jars.packages','org.apache.hadoop:hadoop-common:3.4.1')
                .config("spark.hadoop.fs.azure.account.key.poctrials.dfs.core.windows.net",self.__account_key)
                .config('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
                .config("spark.sql.catalog.iceberg_catalog.warehouse", f"abfss://{self.__container_name}@{self.__storage_account_name}.dfs.core.windows.net/iceberg_catalog/")
                .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
                .getOrCreate()
        )
    

    # def __process_batch(self,df:DataFrame,batch):
    #     print(f'Now processing batch {batch}')
    #     df.show()


    def applyTransformations(self,df:DataFrame) ->DataFrame:

        order_schema = StructType([
            StructField("OrderId", StringType(), False),
            StructField("OrderDate", DateType(), False),
            StructField("CustomerID", StringType(), False),

            StructField("Location", StructType([
                StructField("City", StringType(), False),
                StructField("Pincode", IntegerType(), False),
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
        
        falattened_json_df.show()

        return falattened_json_df




    def launch(self):

        #Set config params described above
        self.spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
        self.spark.conf.set("spark.hadoop.fs.azure.account.auth.type."+self.__storage_account_name +".dfs.core.windows.net", "OAuth")
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth.provider.type."+self.__storage_account_name +".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.id."+self.__storage_account_name +".dfs.core.windows.net", self.__client_id)
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.secret."+self.__storage_account_name +".dfs.core.windows.net", self.__client_secret)
        self.spark.conf.set("spark.hadoop.fs.azure.account.oauth2.client.endpoint."+self.__storage_account_name +".dfs.core.windows.net", "https://login.microsoftonline.com/"+self.__tenant_id+"/oauth2/token")

        path = "abfss://"+self.__container_name+"@"+self.__storage_account_name +".dfs.core.windows.net/iceberg_catalog/"
        
        kafka_df = (
            self.spark
                .read
                .format('kafka')
                .options(**self.kafkaOptions)
                .load()
        )

        flattened_df = self.applyTransformations(kafka_df)

        self.spark.sql('CREATE DATABASE IF NOT EXISTS iceberg_catalog.IcebergDB')

        self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS iceberg_catalog.IcebergDB.orders (
                        OrderId STRING,
                        OrderDate DATE,
                        CustomerID STRING,
                        City STRING,
                        Pincode LONG,
                        Region STRING,
                        ProductID INTEGER,
                        ProductName STRING,
                        Category STRING,
                        Brand STRING,
                        UnitPrice DOUBLE,
                        Discount DOUBLE,
                        TotalUnits INTEGER,
                        POS STRING,
                        OrderStatus STRING,
                        TotalAmount DOUBLE,
                        PaymentMethod STRING,
                        PaymentStatus STRING,
                        TransactionID STRING,
                        Shipping_Street STRING,
                        Shipping_City STRING,
                        Shipping_State STRING,
                        Shipping_ZipCode INTEGER,
                        Shipping_Country STRING,
                        ShippingMethod STRING,
                        EstimatedDelivery DATE,
                        Timestamp TIMESTAMP)
                    USING iceberg 
                """)
        

        # flattened_df.write.format('iceberg').mode('overwrite').option('path',path+'IcebergDB/orders').saveAsTable('iceberg_catalog.IcebergDB.orders')


        (
            flattened_df
            .writeStream
            .outputMode('append')
            .format('iceberg')
            .trigger(processingTime = '10 seconds')
            .option('checkpointLocation',path +'/IcebergDB/checkpoint/')
            .option('table','iceberg_catalog.IcebergDB.orders')
            .option('path',path+'IcebergDB/orders')
            .start()
            .awaitTermination()
        )


if __name__ =='__main__':

    bootstrap_servers = 'redpanda-0:9092'
    topic = 'orders_topic'
    startingOffsets = 'earliest'

    stream = Structuredstream(bootstrap_servers,topic,startingOffsets)

    stream.launch()

# docker exec -it sparkstreaming-spark-master-1 spark-submit --master spark://172.19.0.2:7077 --deploy-mode client --packages org.apache.iceberg:iceberg-spark-runtime-3.3:1.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4  jobs/sparkstream.py