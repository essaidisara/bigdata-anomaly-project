from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    col,
    current_timestamp,
    when,
    window,
    lit,
    date_format
)

# =====================================
# 1. Spark Session
# =====================================
spark = SparkSession.builder \
    .appName("NASA-Logs-Security-Detection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# 2. Lecture depuis Kafka
# =====================================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nasa_logs") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 2000) \
    .load()

logs_df = raw_df.selectExpr("CAST(value AS STRING) as log")

# =====================================
# 3. Parsing des logs NASA
# =====================================
parsed_df = logs_df.select(
    regexp_extract("log", r'^(\S+)', 1).alias("host"),
    regexp_extract("log", r'\[(.*?)\]', 1).alias("log_ts"),
    regexp_extract("log", r'\"(\S+)', 1).alias("method"),
    regexp_extract("log", r'\"(?:\S+)\s(\S+)', 1).alias("endpoint"),
    regexp_extract("log", r'\".*\"\s(\d+)', 1).alias("status"),
    regexp_extract("log", r'\s(\d+)$', 1).alias("bytes")
) \
.withColumn("status", col("status").cast("int")) \
.withColumn("bytes", col("bytes").cast("long")) \
.withColumn("event_time", current_timestamp()) \
.withColumn(
    "timestamp_formatted",
    date_format(current_timestamp(), "dd/MMM/yyyy:HH:mm:ss")
)

# =====================================
# 4. RÈGLES UNITAIRES
# =====================================
unit_rules_df = parsed_df \
.withColumn(
    "severity_level",
    when(col("status") >= 500, 3)
    .when(col("bytes") > 50000, 2)
    .when(col("endpoint").rlike("\\.(mpg|zip|exe)$"), 2)
    .when(col("status") == 404, 1)
    .otherwise(0)
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

# =====================================
# 5. MULTIPLES 404 — MEDIUM
# =====================================
multiple_404_df = unit_rules_df \
    .withWatermark("event_time", "2 hours") \
    .filter(col("status") == 404) \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("host")
    ) \
    .count() \
    .filter(col("count") > 10) \
    .select(
        col("host"),
        lit("MULTIPLE_404").alias("event_type"),
        lit(2).alias("severity_level"),
        lit("MEDIUM").alias("severity_label"),
        current_timestamp().alias("@timestamp")
    )

# =====================================
# 6. TAUX ÉLEVÉ DE REQUÊTES — HIGH
# =====================================
high_rate_df = unit_rules_df \
    .withWatermark("event_time", "2 hours") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("host")
    ) \
    .count() \
    .filter(col("count") > 50) \
    .select(
        col("host"),
        lit("HIGH_REQUEST_RATE").alias("event_type"),
        lit(3).alias("severity_level"),
        lit("HIGH").alias("severity_label"),
        current_timestamp().alias("@timestamp")
    )

# =====================================
# 7. SÉPARATION DES FLUX
# =====================================
normal_logs = unit_rules_df.filter(col("severity_level") == 0)
security_events = unit_rules_df.filter(col("severity_level") > 0)

# =====================================
# 8. ÉCRITURE ELASTICSEARCH
# =====================================
def write_normal(batch_df, batch_id):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .mode("append") \
        .save("nasa-logs")

def write_security(batch_df, batch_id):
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .mode("append") \
        .save("nasa-anomalies")

# =====================================
# 9. STREAMING QUERIES
# =====================================
query_normal = normal_logs.writeStream \
    .foreachBatch(write_normal) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/normal") \
    .start()

query_security = security_events.writeStream \
    .foreachBatch(write_security) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/security") \
    .start()

query_404 = multiple_404_df.writeStream \
    .foreachBatch(write_security) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/multiple_404") \
    .start()

query_rate = high_rate_df.writeStream \
    .foreachBatch(write_security) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/high_rate") \
    .start()

spark.streams.awaitAnyTermination()