from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StringType, FloatType
import traceback
import logging

KAFKA_TOPIC_NAME = "temperature_readings_april19"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
OUTPUT_PATH = "output/csv_data"
logging.basicConfig(filename='./logs/app.log', level=logging.DEBUG, 
                   format="%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s")
logger = logging.getLogger(__name__)

def pull_data_from_kafka():
    global spark
    spark = SparkSession.builder \
        .appName("KafkaTempProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.mysql:mysql-connector-j:8.2.0") \
        .getOrCreate()
          
    schema = (
        StructType()
        .add("device_id", StringType())
        .add("temperature", FloatType())
        .add("client_timestamp", StringType())
        .add("server_timestamp", StringType())
    )
    
    kafka_source = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()
    
    parsed_df = kafka_source.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")
        
    labeled_df = parsed_df.withColumn(
        "status",
        when(col("temperature") > 10.20, "hot").otherwise("cold")
    )
    
    return labeled_df

def write_data_to_csv(df):
    try:
        df.writeStream \
            .format("csv") \
            .option("path", OUTPUT_PATH) \
            .option("checkpointLocation", "output/checkpoints/") \
            .option("header", "true") \
            .trigger(processingTime='5 seconds') \
            .start() \
            .awaitTermination()   
    except Exception as e:
        traceback.print_exc()

def write_data_to_db(df):
    def write_to_mysql(batch_df, batch_id):
        try:
            row_count = batch_df.count()
            logger.info(f"Processing batch {batch_id} with {row_count} records")
            
            if row_count > 0:
                logger.debug("Sample data:")
                logger.debug(batch_df.limit(2).collect())
                
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:mysql://localhost:3306/data_sink") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "temperature_readings") \
                .option("user", "root") \
                .option("password", "root") \
                .mode("append") \
                .save()
        except Exception as e:
            traceback.print_exc()

    try:
        df.writeStream \
            .foreachBatch(write_to_mysql) \
            .outputMode("append") \
            .option("checkpointLocation", "output/mysql_checkpoint/") \
            .trigger(processingTime="2 seconds") \
            .start() \
            .awaitTermination()
    except Exception as e:
        traceback.print_exc()
            
        

if __name__ == "__main__":
    
    df = pull_data_from_kafka()
    df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    #write_data_to_csv(df)
    write_data_to_db(df)
    