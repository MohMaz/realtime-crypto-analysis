import sys
import os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import udf, col

from pyspark.sql.types import DateType
from pyspark.sql.functions import unix_timestamp
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from datetime import datetime


def main():
    
    
    userSchema = types.StructType([
    types.StructField('id', types.IntegerType(), False),
    types.StructField('Date', types.StringType(), False),
    types.StructField('Open', types.FloatType(), False),
    types.StructField('High', types.FloatType(), False),
    types.StructField('Low', types.FloatType(), False),
    types.StructField('Close', types.FloatType(), False),
    types.StructField('Volume', types.IntegerType(), False),
    types.StructField('Market Cap', types.IntegerType(), False)
    ])
    
    cwd = os.getcwd()
    path= outdir = cwd +'/data/tmp'
    lines = spark.readStream.schema(userSchema).format('csv') \
            .option('path', path).load()
            
    lines.printSchema()
    lines=lines.withColumn('timestamp',  functions.to_date(lines['Date'], "MMM d, yyyy"))

    
    lines.printSchema()
    stream = lines.writeStream.format('console') \
            .outputMode('update').start()
    stream.awaitTermination()
    


if __name__ == '__main__':
    main()