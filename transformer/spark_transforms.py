from pyspark import SparkContext
from pyspark.sql import SparkSession

def to_farenheit(row):
    temp_in_farenheit = round(((row.temperature * 9/5) + 32),2)
    return [row.device, temp_in_farenheit]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_transformer") \
        .master("local") \
        .getOrCreate()
    
    sample_df = [
        ["po1",25.31],
        ["po2",34.21]
    ]
    
    df = spark.createDataFrame(data = sample_df, schema = ["device","temperature"])
    
    rdd1 = df.rdd.map(lambda x: to_farenheit(x))
    
    for x in rdd1.collect():
        print(x)