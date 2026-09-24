from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

spark = SparkSession.builder \
    .appName("NASA-Logs-Processing") \
    .getOrCreate()

# Lecture depuis Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nasa_logs") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

logs_df = raw_df.selectExpr("CAST(value AS STRING) as log")

# Parsing Apache log (NASA)
parsed_df = logs_df.select(
    regexp_extract("log", r'^(\S+)', 1).alias("host"),
    regexp_extract("log", r'\[(.*?)\]', 1).alias("timestamp"),
    regexp_extract("log", r'\"(\S+)', 1).alias("method"),
    regexp_extract("log", r'\"(?:\S+)\s(\S+)', 1).alias("endpoint"),
    regexp_extract("log", r'\".*\"\s(\d+)', 1).alias("status"),
    regexp_extract("log", r'\s(\d+)$', 1).alias("bytes")
)

clean_df = parsed_df.withColumn("status", col("status").cast("int")) \
                    .withColumn("bytes", col("bytes").cast("long"))

# Affichage console (test)
query = clean_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .start()

query.awaitTermination()

