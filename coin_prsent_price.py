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


import os
import django
from channels.layers import get_channel_layer


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.backend_pre.config.settings.dev')
django.setup()

channel_layer = get_channel_layer()
 
async def send_data_to_channels_layer(data):
    await channel_layer.group_send(
        'stream_group', 
        {
            'type': 'send_stream_data', 
            'data': data
        }
    )

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType
)
import asyncio
from udf_util import test_straming_preprocessing


schema: StringType = StructType([
    StructField("average", StructType([
        StructField("name", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("data", StructType([
            StructField("opening_price", DoubleType()),
            StructField("closing_price", DoubleType()),
            StructField("max_price", DoubleType()),
            StructField("min_price", DoubleType())
        ]))
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
    .foreach(lambda x: asyncio.ensure_future(send_data_to_channels_layer(x)))\
    .start()
        
query.awaitTermination()

