import os
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import to_json, struct, to_timestamp

spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
kafka_server = '199.60.17.212:9092'
kafka_topic = 'price-demo'


def main():
    # id,Open,High,Low,Close,Volume,Time,Date
    priceSchema = types.StructType([
        types.StructField('id', types.IntegerType(), False),
        types.StructField('Open', types.FloatType(), False),
        types.StructField('High', types.FloatType(), False),
        types.StructField('Low', types.FloatType(), False),
        types.StructField('Close', types.FloatType(), False),
        types.StructField('Volume', types.FloatType(), False),
        types.StructField('Time', types.StringType(), False),
        types.StructField('Date', types.StringType(), False),
    ])

    # column_list =
    cwd = os.getcwd()
    print(cwd)
    path = os.path.join(cwd, 'streaming/data/price')
    lines = spark.readStream.schema(priceSchema).format('csv') \
        .option('path', path).load()

    lines = lines.withColumn('value_json', to_json(struct(lines.columns)))
    lines = lines.selectExpr("CAST(id AS STRING) as key", "CAST(value_json AS STRING) as value")

    # stream = lines.writeStream.format('console').option('truncate', False) \
    #     .outputMode('update').start()
    stream = lines.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", kafka_topic) \
        .option("checkpointLocation", "./spark-checkpoints/price") \
        .start()

    stream.awaitTermination()


if __name__ == '__main__':
    main()
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 data_loading/event_stream_generator.py
