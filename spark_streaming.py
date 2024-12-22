from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark oturumu oluşturma
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Veri şemasını tanımlama
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Adj_Close", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Open", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Price_Change", DoubleType(), True),
    StructField("Anomaly", StringType(), True)
])

# Kafka'dan veri alma
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "anomaly_data").load()

kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
json_df = kafka_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Gerçek zamanlı veri işleme
query = json_df.writeStream.format("console").start()
query.awaitTermination()
