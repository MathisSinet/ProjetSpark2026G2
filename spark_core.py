from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, DoubleType
import pyspark.sql.functions as F
from threading import Thread
import time

OUTPUT_DIR = "streaming_data"

SCHEMA = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("user_city", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_cat", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("action_type", StringType(), False),
    StructField("price", DoubleType(), False)
])

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("SparkProjectG2")
        .master("local[*]")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
)

df = (
    spark.readStream
        .schema(SCHEMA)
        .json(OUTPUT_DIR)
)

agg = (
    df
    .withWatermark("timestamp", "10 minutes")
    .filter(df.action_type == "ACHAT")
    .groupBy(
        F.window("timestamp", "5 minutes")
    )
    .count()
)


writer = (
    agg.writeStream
        .trigger(processingTime="5 seconds")
        .outputMode("update")
        .format("console")
)

def stop_query_after(delay: float, query: StreamingQuery):
    time.sleep(delay)
    query.stop()

def spark_core(delay: float | None = None):
    query = writer.start()
    if delay is not None:
        Thread(target=stop_query_after, args = (30, query)).start()
    query.awaitTermination()

if __name__ == "__main__":
    spark_core()