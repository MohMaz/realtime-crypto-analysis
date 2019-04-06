import os
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')


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
    path = os.path.join(cwd, 'streaming/data/')
    lines = spark.readStream.schema(priceSchema).format('csv') \
        .option('path', path).load()

    lines.printSchema()

    # lines = lines.withColumn('timestamp', functions.to_date(lines['Date'], "MMM d, yyyy"))

    # lines.printSchema()
    # lines = lines.withColumn('msg', to_json)

    lines = lines.select(to_json(struct(lines.columns)))
    lines = lines.withColumnRenamed(lines.columns[0], 'msg')

    stream = lines.writeStream.format('console').option('truncate', False) \
        .outputMode('update').start()
    stream.awaitTermination()


if __name__ == '__main__':
    main()
