from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType
)


average_schema: StructType = StructType([
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


# JSON 객체 추출
schema: StructType = StructType([
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