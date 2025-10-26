from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("KafkaStreamProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType()\
    .add("order_id", IntegerType())\
    .add("product", StringType())\
    .add("quantity", IntegerType())\
    .add("price", DoubleType())\
    .add("timestamp", StringType())

raw_stream = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "transactions")\
    .load()

parsed = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

agg = parsed.groupBy("product").agg(
    count("order_id").alias("orders"),
    avg("price").alias("avg_price")
)

query = agg.writeStream.outputMode("complete")\
    .format("console")\
    .start()

query.awaitTermination()
