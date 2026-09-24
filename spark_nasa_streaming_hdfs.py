from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    col,
    current_timestamp,
    when,
    lit
)

# ======================================================
# 1. Spark Session
# ======================================================
spark = SparkSession.builder \
    .appName("NASA-Logs-Streaming-HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ======================================================
# 2. Lecture depuis Kafka
# ======================================================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nasa_logs") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 2000) \
    .load()

logs_df = raw_df.selectExpr("CAST(value AS STRING) AS log")

# ======================================================
# 3. Parsing des logs NASA
# ======================================================
parsed_df = logs_df.select(
    regexp_extract("log", r'^(\S+)', 1).alias("host"),
    regexp_extract("log", r'\[(.*?)\]', 1).alias("raw_ts"),
    regexp_extract("log", r'\"(\S+)', 1).alias("method"),
    regexp_extract("log", r'\"(?:\S+)\s(\S+)', 1).alias("endpoint"),
    regexp_extract("log", r'\".*\"\s(\d+)', 1).alias("status"),
    regexp_extract("log", r'\s(\d+)$', 1).alias("bytes")
).withColumn("status", col("status").cast("int")) \
 .withColumn("bytes", col("bytes").cast("long")) \
 .withColumn("event_time", current_timestamp())

# ======================================================
# 4. RÈGLES UNITAIRES
# ======================================================
unit_rules_df = parsed_df \
.withColumn(
    "severity_level",
    when(col("status") >= 500, 3)                 # HIGH
    .when(col("bytes") > 50000, 2)                # MEDIUM
    .when(col("endpoint").rlike("\\.(mpg|zip|exe)$"), 2)
    .when(col("status") == 404, 1)                # LOW
    .otherwise(0)                                 # INFO
) \
.withColumn(
    "severity_label",
    when(col("severity_level") == 3, "HIGH")
    .when(col("severity_level") == 2, "MEDIUM")
    .when(col("severity_level") == 1, "LOW")
    .otherwise("INFO")
) \
.withColumn(
    "event_type",
    when(col("status") >= 500, "HTTP_5XX")
    .when(col("bytes") > 20000, "HIGH_BYTES")
    .when(col("endpoint").rlike("\\.(mpg|zip|exe)$"), "SENSITIVE_FILE_ACCESS")
    .when(col("status") == 404, "HTTP_404")
    .otherwise("NORMAL_EVENT")
)

# ======================================================
# 5. SÉPARATION DES FLUX
# ======================================================
normal_events_df = unit_rules_df.filter(col("severity_level") == 0)

anomalies_df = unit_rules_df.filter(col("severity_level") > 0)

# ======================================================
# 6. ÉCRITURE HDFS — NORMAL EVENTS
# ======================================================
query_normal = normal_events_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/data/nasa/processed") \
    .option("checkpointLocation", "/data/nasa/checkpoints/processed") \
    .start()

# ======================================================
# 7. ÉCRITURE HDFS — ANOMALIES
# ======================================================
query_anomalies = anomalies_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/data/nasa/anomalies") \
    .option("checkpointLocation", "/data/nasa/checkpoints/anomalies") \
    .start()

# ======================================================
# 8. LANCEMENT DU STREAMING
# ======================================================
spark.streams.awaitAnyTermination()
