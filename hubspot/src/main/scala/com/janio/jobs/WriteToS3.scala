package com.janio.jobs

import com.janio.connection.SparkInstance
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.slf4j.{Logger, LoggerFactory}

object WriteToS3 {
  val infoLogger: Logger = LoggerFactory.getLogger(WriteToS3.getClass)
  val config: Config = ConfigFactory.load("config")

  def main(args: Array[String]): Unit = {
    val spark = SparkInstance.getSparkInstance()
    import spark.implicits._

    /**
      * Creating dataframe from kafka
      */
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("KAFKA_BOOTSTRAP_SERVERS"))
      .option("subscribe", config.getString("KAFKA_TOPIC"))
      .option("startingOffsets","earliest")
      .load()
      .selectExpr("CAST(value AS STRING)","offset")
      .as[(String, Long)]
      .withColumn("source", get_json_object(col("value"), "$.source"))
      .withColumn("table", get_json_object(col("source"), "$.table"))
      .withColumn("event_timestamp", get_json_object(col("value"), "$.ts_ms").$div(1000)
        .cast(TimestampType))
      .withColumn("lsn",get_json_object(col("source"), "$.lsn").cast(LongType))
      .withColumn("event_type", when(get_json_object(col("value"), "$.before").isNull === true, "insert")
        .when(get_json_object(col("value"), "$.before").isNotNull === true
          && get_json_object(col("value"), "$.after").isNotNull === true, "update")
        .when(get_json_object(col("value"), "$.after").isNull === true, "delete"))
      .withColumn("value", when(get_json_object(col("value"), "$.after").isNull === true,
        get_json_object(col("value"), "$.before"))
        .otherwise(get_json_object(col("value"), "$.after")))
      .withColumn("j_date", to_date(col("event_timestamp"),"yyyy-MM-dd"))
      .drop("source")

    /**
     * Writing data for janio_order_service table
     */
    val janio_order_service= df.filter("table='janio_order_service'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioOrderService)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_service/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_service/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    janio_order_service.awaitTermination()
  }

}
