from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, current_timestamp

spark = SparkSession.builder \
    .appName("NASA-Logs-To-ES") \
    .getOrCreate()

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nasa_logs") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 2000) \
    .load()

logs_df = raw_df.selectExpr("CAST(value AS STRING) as log")

parsed_df = logs_df.select(
    regexp_extract("log", r'^(\S+)', 1).alias("host"),
    regexp_extract("log", r'\[(.*?)\]', 1).alias("raw_ts"),
    regexp_extract("log", r'\"(\S+)', 1).alias("method"),
    regexp_extract("log", r'\"(?:\S+)\s(\S+)', 1).alias("endpoint"),
    regexp_extract("log", r'\".*\"\s(\d+)', 1).alias("status"),
    regexp_extract("log", r'\s(\d+)$', 1).alias("bytes"),
).withColumn("status", col("status").cast("int")) \
 .withColumn("bytes", col("bytes").cast("long")) \
 .withColumn("@timestamp", current_timestamp())

def write_to_es(batch_df, batch_id):
    (batch_df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", "elasticsearch")
        .option("es.port", "9200")
        .option("es.nodes.wan.only", "true")
        .mode("append")
        .save("nasa-logs"))

query = parsed_df.writeStream.foreachBatch(write_to_es).outputMode("append").start()
query.awaitTermination()

