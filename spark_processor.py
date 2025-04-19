from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, to_json, struct
from pyspark.sql.types import StructType, StringType, FloatType
import traceback

KAFKA_TOPIC_NAME = "temperature_readings_april19"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaTempProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
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
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = kafka_source.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")
        
    labeled_df = parsed_df.withColumn(
        "status",
        when(col("temperature") > 10.20, "hot").otherwise("cold")
    )
    
    # output_df = labeled_df.select(
    #     to_json(struct("*"))
    # )

# write stream to csv
    try:
        labeled_df.writeStream \
            .format("csv") \
            .option("path", "output/csv_data") \
            .option("checkpointLocation", "output/checkpoints/") \
            .option("header", "true") \
            .trigger(processingTime='5 seconds') \
            .start() \
            .awaitTermination()   
    except Exception as e:
        traceback.print_exc()
    
    