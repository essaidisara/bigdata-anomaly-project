from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSparkTest") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "web_logs") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING)"
    )

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

