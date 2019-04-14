import os
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import to_timestamp, to_json, struct

spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
kafka_server = '199.60.17.212:9092'
kafka_topic = 'news-demo'


def main():
    # id,web_url, content, headline, document_type, pub_date, source, section_name, author, word_count, content_no_stopwords,
    # SENTIMENT_VALUE, SENTIMENT, relevance, score
    priceSchema = types.StructType([
        types.StructField('id', types.IntegerType(), False),
        types.StructField('web_url', types.StringType(), False),
        types.StructField('content', types.StringType(), False),
        types.StructField('headline', types.StringType(), False),
        types.StructField('document_type', types.StringType(), False),
        types.StructField('pub_date', types.StringType(), False),
        types.StructField('source', types.StringType(), False),
        types.StructField('section_name', types.StringType(), False),
        types.StructField('author', types.StringType(), False),
        types.StructField('word_count', types.StringType(), False),
        types.StructField('content_no_stopwords', types.StringType(), False),
        types.StructField('SENTIMENT_VALUE', types.FloatType(), False),
        types.StructField('SENTIMENT', types.StringType(), False),
        types.StructField('relevance', types.FloatType(), False),
        types.StructField('score', types.FloatType(), False),
    ])

    # column_list =
    cwd = os.getcwd()
    print(cwd)
    path = os.path.join(cwd, 'streaming/data/news')
    lines = spark.readStream.schema(priceSchema).format('csv') \
        .option('path', path).load()

    lines.printSchema()
    lines.printSchema()
    lines = lines.withColumn('timestamp', to_timestamp(lines['pub_date']))
    lines = lines.withColumn('value_json', to_json(struct(lines.columns)))
    lines = lines.selectExpr("CAST(id AS STRING) as key", "CAST(value_json AS STRING) as value")
    lines.printSchema()

    # stream = lines.writeStream.format('console').option('truncate', False) \
    #     .outputMode('update').start()
    stream = lines.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", kafka_topic) \
        .option("checkpointLocation", "./spark-checkpoints/news") \
        .start()

    stream.awaitTermination()


if __name__ == '__main__':
    main()
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 streaming/streaming_news.py
