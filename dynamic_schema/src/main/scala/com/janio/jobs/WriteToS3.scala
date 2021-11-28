package com.janio.jobs

import com.janio.connection.SparkInstance
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, get_json_object, to_date, when}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.slf4j.{Logger, LoggerFactory}

object WriteToS3 {
  val infoLogger: Logger = LoggerFactory.getLogger(WriteToS3.getClass)
  val config: Config = ConfigFactory.load("config")

  def main(args: Array[String]): Unit = {
    val spark = SparkInstance.getSparkInstance()

    /**
      * Creating dataframe from kafka
      */
//    val df = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", config.getString("KAFKA_BOOTSTRAP_SERVERS"))
//      .option("subscribe", config.getString("KAFKA_TOPIC"))
//      .option("startingOffsets","earliest")
//      .load()
//      .selectExpr("CAST(value AS STRING)","offset")
//      .as[(String, Long)]
//      .withColumn("source", get_json_object(col("value"), "$.payload.payload.source"))
//      .withColumn("table", get_json_object(col("source"), "$.payload.payload.table"))
//      .withColumn("event_timestamp", get_json_object(col("value"), "$.payload.payload.ts_ms").$div(1000)
//        .cast(TimestampType))
//      .withColumn("lsn",get_json_object(col("source"), "$.payload.payload.lsn").cast(LongType))
//      .withColumn("event_type", when(get_json_object(col("value"), "$.payload.payload.before").isNull === true, "insert")
//        .when(get_json_object(col("value"), "$.payload.payload.before").isNotNull === true
//          && get_json_object(col("value"), "$.payload.payload.after").isNotNull === true, "update")
//        .when(get_json_object(col("value"), "$.payload.payload.after").isNull === true, "delete"))
//      .withColumn("value", when(get_json_object(col("value"), "$.payload.payload.after").isNull === true, get_json_object(col("value"), "$.payload.payload.before"))
//        .otherwise(get_json_object(col("value"), "$.payload.payload.after")))
//      .withColumn("j_date", to_date(col("event_timestamp"),"yyyy-MM-dd"))
//      .drop("source")


    /**
      * Writing data for janio_order_service table
      */
//    val janio_order_service= df.filter("table='janio_order_service'").withColumn("value", from_json(col("value"),
//      JanioBacknedSchema.janioOrderService)).select("value.*","event_type","event_timestamp","j_date","lsn")
//      .repartition(1)
//      .writeStream
//      .outputMode("append")
//      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_service/")
//      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_service/")
//      .option("orc.compress", "zlib")
//      .format("orc")
//      .partitionBy("j_date")
//      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
//      .start()

//    janio_order_service.awaitTermination()


    val df = spark.read.textFile("/Users/tusharmudgal/Desktop/ETLkafka/dynamic_schema/src/main/resources/test.txt")
      .withColumn("schema", get_json_object(col("value"), "$.schema"))
      .withColumn("payload", get_json_object(col("value"), "$.payload"))
      .withColumn("before", get_json_object(col("payload"), "$.before"))
      .withColumn("after", get_json_object(col("payload"), "$.after"))
      .withColumn("table", get_json_object(col("payload"), "$.source.table"))
      .withColumn("event_timestamp", get_json_object(col("payload"), "$.ts_ms").$div(1000)
        .cast(TimestampType))
      .withColumn("lsn", get_json_object(col("payload"), "$.source.lsn").cast(LongType))
      .withColumn("event_type",
        when(get_json_object(col("payload"), "$.before").isNull === true, "insert")
        .when(get_json_object(col("payload"), "$.before").isNotNull === true
          && get_json_object(col("payload"), "$.after").isNotNull === true, "update")
        .when(get_json_object(col("payload"), "$.after").isNull === true, "delete"))
      .withColumn("value", when(get_json_object(col("value"), "$.after").isNull === true,
        get_json_object(col("payload"), "$.before"))
        .otherwise(get_json_object(col("payload"), "$.after")))
      .withColumn("j_date", to_date(col("event_timestamp"),"yyyy-MM-dd"))
      .drop("payload")
      .drop("value")


//    df.show(false)

  }

}
