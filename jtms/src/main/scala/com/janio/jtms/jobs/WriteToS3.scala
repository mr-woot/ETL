package com.janio.jtms.jobs

import com.janio.jtms.connection.SparkInstance
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, TimestampType}
import org.slf4j.LoggerFactory

object WriteToS3 {
  val infoLogger = LoggerFactory.getLogger(WriteToS3.getClass)
  val config = ConfigFactory.load("config")

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
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]
      .withColumn("source", get_json_object(col("value"), "$.source"))
      .withColumn("table", get_json_object(col("source"), "$.table"))
      .withColumn("event_type", when(get_json_object(col("value"), "$.before").isNull === true, "insert")
        .when(get_json_object(col("value"), "$.before").isNotNull === true
          && get_json_object(col("value"), "$.after").isNotNull === true, "update")
        .when(get_json_object(col("value"), "$.after").isNull === true, "delete"))
      .withColumn("event_timestamp", get_json_object(col("value"), "$.ts_ms").$div(1000).cast(TimestampType))
      .withColumn("value", when(get_json_object(col("value"), "$.after").isNull === true, get_json_object(col("value"), "$.before"))
        .otherwise(get_json_object(col("value"), "$.after")))
      .withColumn("j_date", to_date(col("event_timestamp"), "yyyy-MM-dd"))
      .drop("source")


    /**
      * Writing data for shipments table
      */
    val shipments = df.filter("table='shipments'").withColumn("value", from_json(col("value"),
      JTMSSchema.shipments)).select("value.*", "event_type", "event_timestamp", "j_date")
      .withColumn("created_at", $"created_at".cast(TimestampType))
      .withColumn("updated_at", $"updated_at".cast(TimestampType))
      .withColumn("assigned_date", $"assigned_date".cast(TimestampType))
      .withColumn("completed_date", $"completed_date".cast(TimestampType))
      .withColumn("scheduled_pickup_date", expr("date_add('1970-01-01',scheduled_pickup_date)"))
      .withColumn("scheduled_delivery_date", expr("date_add('1970-01-01',scheduled_delivery_date)"))
      .withColumn("reschedule_date", expr("date_add('1970-01-01',reschedule_date)"))
      .withColumn("pickup_or_delivery_date", expr("date_add('1970-01-01',pickup_or_delivery_date)"))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH") + "shipments/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT") + "shipments/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start();

    /**
      * Writing data for shipment-trackers table
      */
    val shipmentTrackers = df.filter("table='shipment-trackers'").withColumn("value", from_json(col("value"),
      JTMSSchema.shipmenttrackers)).select("value.*", "event_type", "event_timestamp", "j_date")
      .withColumn("created_at", $"created_at".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH") + "shipment-trackers/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT") + "shipment-trackers/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start();

    /**
      * Writing data for drivers table
      */
    val drivers = df.filter("table='drivers'").withColumn("value", from_json(col("value"),
      JTMSSchema.drivers)).select("value.*", "event_type", "event_timestamp", "j_date")
      .withColumn("created_at", $"created_at".cast(TimestampType))
      .withColumn("updated_at", $"updated_at".cast(TimestampType))
      .withColumn("deactivate_from_date", expr("date_add('1970-01-01',deactivate_from_date)"))
      .withColumn("deactivate_to_date", expr("date_add('1970-01-01',deactivate_to_date)"))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH") + "drivers/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT") + "drivers/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start();

    /**
      * Writing data for users table
      */
    val users = df.filter("table='users'")
      .withColumn("value", from_json(col("value"),
      JTMSSchema.users)).select("value.*", "event_type", "event_timestamp", "j_date")
      .withColumn("created_at", $"created_at".cast(TimestampType))
      .withColumn("updated_at", $"updated_at".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH") + "users/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT") + "users/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start();


    shipments.awaitTermination()
    shipmentTrackers.awaitTermination()
    drivers.awaitTermination()
    users.awaitTermination()
  }

}
