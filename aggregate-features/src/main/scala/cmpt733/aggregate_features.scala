package cmpt733

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.types.{ArrayType, ObjectType, StringType, StructType}
import org.apache.spark.sql.functions._


object aggregate_features {


  val schema = new StructType()
    .add("timestamp", StringType)
    .add("msg_type", StringType)
    .add("data", ArrayType(StringType))

  case class event(timestamp: String, msg_type: String, data: Array[String])

  case class prediction_state(timestamp: String, price_list: Array[String], news_list: Array[String])

  case class prediction_row(timestamp: String, data: Array[String])

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("Aggregator")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    //    val df = spark
    //      .readStream
    //      .format("text")
    //      .load("sample_dataset")
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "199.60.17.212:9092")
      .option("subscribe", "test3")
      .option("maxOffsetsPerTrigger", 5)
      .option("startingOffsets", """{"test3":{"0":0}}""")
      .load()

    val df2 = df
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .select(from_json('value, schema) as 'e)
      .select($"e.timestamp", $"e.msg_type", $"e.data")
      .as[(String, String, Array[String])]
      .map(row => event(row._1, row._2, row._3))
      .groupByKey(_.timestamp)
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(updateAcrossEvents)


    df2.printSchema()

    val query = df2.writeStream
      .format("console")
      .option("truncate", false) // <-- requests ConsoleSinkProvider for a sink
      .trigger(Trigger.ProcessingTime(2000))

    query.start().awaitTermination()
  }

  def updateAcrossEvents(domain_sessionid: String,
                         inputs: Iterator[event],
                         oldState: GroupState[prediction_state]): prediction_row = {

    var state: prediction_state = if (oldState.exists) oldState.get else prediction_state("NULL", null, null)

    var timeoutTimestamp = new java.sql.Timestamp(0L)


    if (oldState.hasTimedOut) {
      val finalState = burialUserStateForTimeOut(state)
      oldState.remove()
      finalState
    }
    else {
      for (input <- inputs) {
        state = updateUserStateWithEvent(state, input)
        oldState.update(state)
        if (timeoutTimestamp.before(input.collector_tstamp)) {
          timeoutTimestamp = input.collector_tstamp
        }
      }
      //oldState.setTimeoutDuration("30 minutes")
      oldState.setTimeoutTimestamp(timeoutTimestamp.getTime, "30 minutes")
      state
    }
  }

}
