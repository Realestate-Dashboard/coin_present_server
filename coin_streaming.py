import sys
from pathlib import Path

# 현재 파일의 경로
file_path = Path(__file__).resolve()
# 상위 경로 backend
parent_path = file_path.parent

# 상위 경로 backed_pre -> sys 경로 추가
grandparent_path = parent_path.parent
backend__ = f"{str(grandparent_path)}/backend/backend_pre"
sys.path.append(str(parent_path))
sys.path.append(str(grandparent_path))
sys.path.append(backend__)


import asyncio
from typing import *
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from constructure import average_schema, schema
from udf_util import test_straming_preprocessing
from pyspark.sql.functions import udf


spark_conf = SparkConf().setAppName("MyApp")\
    .setMaster("local[*]")\
    .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \

spark = SparkSession.builder.config(conf=spark_conf)\
                            .getOrCreate()    


def spark_average_coin_price(topics: str):
    average_udf = udf(test_straming_preprocessing, average_schema)

    # 스트리밍 데이터 생성
    stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092") \
        .option("subscribe", topics) \
        .option("startingOffsets", "earliest") \
        .load()

    data_df = stream_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json("value", schema=schema).alias("crypto"))\
        .selectExpr("crypto.upbit.data as upbit_price", 
                    "crypto.bithum.data as bithum_price", 
                    "crypto.korbit.data as korbit_price")\
        .withColumn("total_price", average_udf(col("upbit_price"), 
                                               col("bithum_price"), 
                                               col("korbit_price")).alias("average_price"))\
        .select("total_price")
    
    return data_df


def run_spark_stream(name: str, topics: str, send_topic_name: str) -> None:
    data_df = spark_average_coin_price(topics=topics)
    query = data_df.writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092") \
        .option("topic", send_topic_name) \
        .option("checkpointLocation", f".checkpoint_{name}")\
        .foreach(print)\
        .start()
    query.awaitTermination()
        
        
run_spark_stream(name="bitcoin" ,topics='trade_bitcoin_total', send_topic_name="average_bitcoin_price")
