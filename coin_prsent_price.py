from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType
)

from ss import test_straming_preprocessing


schema = StructType([
    StructField("name", StringType()),
    StructField("timestamp", DoubleType()),
    StructField("data", StructType([
        StructField("opening_price", DoubleType()),
        StructField("closing_price", DoubleType()),
        StructField("max_price", DoubleType()),
        StructField("min_price", DoubleType())
    ]))
])
average_udf = udf(test_straming_preprocessing, schema)


# JSON 객체 추출
schema = StructType([
    StructField("upbit", StructType([
        StructField("name", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("data", StructType([
            StructField("opening_price", DoubleType()),
            StructField("closing_price", DoubleType()),
            StructField("max_price", DoubleType()),
            StructField("min_price", DoubleType())
        ]))
    ])),
    StructField("bithum", StructType([
        StructField("name", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("data", StructType([
            StructField("opening_price", DoubleType()),
            StructField("closing_price", DoubleType()),
            StructField("max_price", DoubleType()),
            StructField("min_price", DoubleType())
        ]))
    ])),
    StructField("korbit", StructType([
        StructField("name", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("data", StructType([
            StructField("opening_price", DoubleType()),
            StructField("closing_price", DoubleType()),
            StructField("max_price", DoubleType()),
            StructField("min_price", DoubleType())
        ]))
    ])),
    
])

spark_conf = SparkConf().setAppName("MyApp")\
    .setMaster("local[*]")\
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \


spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()


# 스트리밍 데이터 생성
stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092") \
    .option("subscribe", "trade_bitcoin_total") \
    .option("startingOffsets", "earliest") \
    .load()


data_df = stream_df.selectExpr("CAST(value AS STRING)")\
    .select(from_json("value", schema=schema).alias("crypto"))\
    .selectExpr("crypto.upbit.data as upbit_price", 
                "crypto.bithum.data as bithum_price", 
                "crypto.korbit.data as korbit_price")\
    .withColumn("total_price", 
                average_udf(col("upbit_price"), col("bithum_price"), col("korbit_price")).alias("average_price"))


result_df = data_df.select("total_price")
query = result_df.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092") \
    .option("topic", "trade_bitcoin_total") \
    .option("checkpointLocation", ".checkpoint")\
    .foreach(print)\
    .start()


query.awaitTermination()

