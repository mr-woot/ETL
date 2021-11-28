package com.janio.jobs

import com.janio.connection.SparkInstance
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, LongType, TimestampType}
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
      .withColumn("value", when(get_json_object(col("value"), "$.after").isNull === true, get_json_object(col("value"), "$.before"))
        .otherwise(get_json_object(col("value"), "$.after")))
      .withColumn("j_date", to_date(col("event_timestamp"),"yyyy-MM-dd"))
      .drop("source")




    /**
      * Writing data for janio_order_order table
      */
   val janio_order_order= df.filter("table='janio_order_order'")
     .withColumn("value", from_json(col("value"),
     JanioBacknedSchema.janioOrderOrder())).select("value.*","event_type","event_timestamp","j_date","lsn")
     .withColumn("created_on",  $"created_on".cast(TimestampType))
     .withColumn("updated_on",  $"updated_on".cast(TimestampType))
     .withColumn("submit_external_service_datetime",  $"submit_external_service_datetime".cast(TimestampType))
     .withColumn("submit_firstmile_datetime",  $"submit_firstmile_datetime".cast(TimestampType))
     .withColumn("submit_warehouse_datetime",  $"submit_warehouse_datetime".cast(TimestampType))
     .withColumn("private_tracker_updated_on",  $"private_tracker_updated_on".cast(TimestampType))
     .withColumn("tracker_updated_on",  $"tracker_updated_on".cast(TimestampType))
     .withColumn("est_completion_date",  $"est_completion_date".cast(TimestampType))
     .withColumn("est_early_completion_date",  $"est_early_completion_date".cast(TimestampType))
     .select("order_id","tracking_no","hawb_no","consignee_name","consignee_address","consignee_postal",
       "consignee_country","consignee_city","consignee_state","order_length","order_width","order_height","order_weight",
       "created_on","updated_on","pickup_address","agent_id_id","agent_application_id_id","service_id_id","warehouse_address",
       "status_code","tracker_status_code","pickup_country","payment_type","shipper_order_id","submit_external_service_datetime",
       "submit_firstmile_datetime","submit_warehouse_datetime","print_url","invoice_created","upload_batch_no","consignee_province",
       "cod_amt_to_collect","tracker_main_text","store_id","delivery_note","pickup_city","pickup_contact_name","pickup_contact_number"
       ,"pickup_postal","pickup_province","pickup_state","incoterm","print_default_label","is_processing","shipper_sub_account_id",
       "shipper_sub_order_id","model_log_link_id","order_label_url","additional_data","private_tracker_status_code",
       "private_tracker_updated_on","tracker_updated_on","est_completion_date","forward_order_id","service_level","is_return_order"
       ,"returns_consolidation_period","latest_private_update_id","latest_update_id","event_type","event_timestamp","lsn",
       "latest_private_update_v2_id","latest_public_update_v2_id","latest_public_update_v2_shipper_id","redelivery_forward_order_id"
       ,"failed_delivery_count","est_early_completion_date","order_type","j_date")
     .repartition(1)
     .writeStream
     .outputMode("append")
     .option("path", config.getString("S3_WRITE_PATH")+"janio_order_order/")
     .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_order/")
     .option("orc.compress", "zlib")
     .format("orc")
     .partitionBy("j_date")
     .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
     .start()


    /**
      * Writing data for janio_tracker_update table
      */
    val janio_tracker_update= df.filter("table='janio_tracker_update'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioTrackerUpdate())).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .withColumn("updated_on",  $"updated_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_tracker_update/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_tracker_update/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()



    /**
      * Writing data for janio_order_agentapplication table
      */
    val janio_order_agentapplication= df.filter("table='janio_order_agentapplication'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioOrderAgentapplication())).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_agentapplication/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_agentapplication/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()


    /**
      * Writing data for janio_tracker_privateupdate table
      */
    val janio_tracker_privateupdate= df.filter("table='janio_tracker_privateupdate'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioTrackerPrivateUpdate)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("updated_on",  $"updated_on".cast(TimestampType))
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_tracker_privateupdate/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_tracker_privateupdate/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()


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



    /**
      * Writing data for janio_payment_paymentsettings table
      */
    val janio_payment_paymentsettings= df.filter("table='janio_payment_paymentsettings'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioPaymentPaymentSettings)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_payment_paymentsettings/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_payment_paymentsettings/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_payment_paymentsettings_pricing_groups table
      */
    val janio_payment_paymentsettings_pricing_groups= df.filter("table='janio_payment_paymentsettings_pricing_groups'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioPaymentSettingsPricingGroups)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_payment_paymentsettings_pricing_groups/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_payment_paymentsettings_pricing_groups/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_payment_pricinggroup table
      */
    val janio_payment_pricinggroup= df.filter("table='janio_payment_pricinggroup'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioPaymentPricingGroup)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_payment_pricinggroup/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_payment_pricinggroup/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_partner_partner table
      */
    val janio_partner_partner= df.filter("table='janio_partner_partner'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioPartnerPartner)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_partner_partner/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_partner_partner/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_order_item table
      */
    val janio_order_item= df.filter("table='janio_order_item'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioOrderItem)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_item/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_item/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_order_orderpickupdetails table
      */
    val janio_order_orderpickupdetails= df.filter("table='janio_order_orderpickupdetails'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioOrderOrderPickUpDetails)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("pickup_date",expr("date_add('1970-01-01',pickup_date)"))
      .withColumn("pickup_time_from",date_format($"pickup_time_from".$div(1000000).cast(DataTypes.TimestampType),"HH:mm:ss"))
      .withColumn("pickup_time_to",date_format($"pickup_time_to".$div(1000000).cast(DataTypes.TimestampType),"HH:mm:ss"))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_orderpickupdetails/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_orderpickupdetails/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_analytics_ordertrackerinfoanalytics table
      */
    val janio_analytics_ordertrackerinfoanalytics= df.filter("table='janio_analytics_ordertrackerinfoanalytics'")
      .withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioAnalyticsOrderTrackerInfoAnalytics)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("analytics_created_on",  $"analytics_created_on".cast(TimestampType))
      .withColumn("analytics_updated_on",  $"analytics_updated_on".cast(TimestampType))
      .withColumn("attempt_1_on",  $"attempt_1_on".cast(TimestampType))
      .withColumn("attempt_2_on",  $"attempt_2_on".cast(TimestampType))
      .withColumn("attempt_3_on",  $"attempt_3_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_analytics_ordertrackerinfoanalytics/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_analytics_ordertrackerinfoanalytics/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_sla_orderslazone table
      */
    val janio_sla_orderslazone= df.filter("table='janio_sla_orderslazone'").withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioSLAOrdersLAZone)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_sla_orderslazone/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_sla_orderslazone/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_location_zone table
      */
    val janio_location_zone= df.filter("table='janio_location_zone'").withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioLocationZone)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_location_zone/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_location_zone/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_order_orderzone table
      */
    val janio_order_orderzone= df.filter("table='janio_order_orderzone'")
      .withColumn("value", from_json(col("value"),
      JanioBacknedSchema.janioOrderOrderZone)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_orderzone/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_orderzone/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_cod_orderremittancedata table
      */
    val janio_cod_orderremittancedata= df.filter("table='janio_cod_orderremittancedata'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioCodOrderRemittanceData)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_cod_orderremittancedata/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_cod_orderremittancedata/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_order_externalorderinfo table
      */
    val janio_order_externalorderinfo= df.filter("table='janio_order_externalorderinfo'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioOrderExternalorderinfo)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_externalorderinfo/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_externalorderinfo/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_tracker_publicupdatev2 table
      */
    val janio_tracker_publicupdatev2= df.filter("table='janio_tracker_publicupdatev2'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioTrackerPublicupdatev2)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_tracker_publicupdatev2/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_tracker_publicupdatev2/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_sla_partnerdelay table
      */
    val janio_sla_partnerdelay= df.filter("table='janio_sla_partnerdelay'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioSlaPartnerdelay)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("response_date",expr("date_add('1970-01-01',response_date)"))
      .withColumn("revised_estimated_completion_date",expr("date_add('1970-01-01',revised_estimated_completion_date)"))
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_sla_partnerdelay/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_sla_partnerdelay/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
      * Writing data for janio_order_orderpickupdatelog table
      */
    val janio_order_orderpickupdatelog= df.filter("table='janio_order_orderpickupdatelog'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioOrderOrderpickupdatelog)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("pickup_date",expr("date_add('1970-01-01',pickup_date)"))
      .withColumn("confirmed_on",  $"confirmed_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_orderpickupdatelog/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_orderpickupdatelog/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_order_weightreconrecord table
     */
    val janio_order_weightreconrecord= df.filter("table='janio_order_weightreconrecord'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioOrderWeightreconrecord)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_weightreconrecord/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_weightreconrecord/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_tracker_partnerupdate table
     */
    val janio_tracker_partnerupdate= df.filter("table='janio_tracker_partnerupdate'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioTrackerPartnerupdate)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("updated_on",  $"updated_on".cast(TimestampType))
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_tracker_partnerupdate/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_tracker_partnerupdate/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_activity_orderactivity table
     */
    val janio_activity_orderactivity= df.filter("table='janio_activity_orderactivity'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioActivityOrderactivity)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("activity_on",  $"activity_on".cast(TimestampType))
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_activity_orderactivity/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_activity_orderactivity/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_tracker_privateupdatev1v2link table
     */
    val janio_tracker_privateupdatev1v2link= df.filter("table='janio_tracker_privateupdatev1v2link'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioTrackerPrivateupdatev1v2link)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_tracker_privateupdatev1v2link/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_tracker_privateupdatev1v2link/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_tracker_privateupdatev2 table
     */
    val janio_tracker_privateupdatev2= df.filter("table='janio_tracker_privateupdatev2'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janio_tracker_privateupdatev2)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("updated_on",  $"updated_on".cast(TimestampType))
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_tracker_privateupdatev2/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_tracker_privateupdatev2/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_midmile_tracker_masterairwaybill_orders table
     */
    val janio_midmile_tracker_masterairwaybill_orders= df.filter("table='janio_midmile_tracker_masterairwaybill_orders'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioMidmileTrackerMasterairwaybillOrders)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_midmile_tracker_masterairwaybill_orders/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_midmile_tracker_masterairwaybill_orders/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_midmile_tracker_houseairwaybill_orders table
     */
    val janio_midmile_tracker_houseairwaybill_orders= df.filter("table='janio_midmile_tracker_houseairwaybill_orders'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioMidmileTrackerHouseairwaybillOrders)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_midmile_tracker_houseairwaybill_orders/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_midmile_tracker_houseairwaybill_orders/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_hms_session table
     */
    val janio_hms_session= df.filter("table='janio_hms_session'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioHmsSession)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_hms_session/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_hms_session/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_hms_sessionorder table
     */
    val janio_hms_sessionorder= df.filter("table='janio_hms_sessionorder'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioHmsSessionorder)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .withColumn("updated_on",  $"updated_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_hms_sessionorder/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_hms_sessionorder/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_hms_sessionorderlog table
     */
    val janio_hms_sessionorderlog= df.filter("table='janio_hms_sessionorderlog'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioHmsSessionorderlog)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_hms_sessionorderlog/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_hms_sessionorderlog/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_order_orderlink table
     */
    val janio_order_orderlink= df.filter("table='janio_order_orderlink'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioOrderOrderlink)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_orderlink/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_orderlink/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_hms_temporarygroup table
     */
    val janio_hms_temporarygroup= df.filter("table='janio_hms_temporarygroup'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioHmsTemporarygroup)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_hms_temporarygroup/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_hms_temporarygroup/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_hms_temporarygrouporders table
     */
    val janio_hms_temporarygrouporders= df.filter("table='janio_hms_temporarygrouporders'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioHmsTemporarygrouporders)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_hms_temporarygrouporders/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_hms_temporarygrouporders/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_hms_temporaryorder table
     */
    val janio_hms_temporaryorder= df.filter("table='janio_hms_temporaryorder'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioHmsTemporaryorder)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .withColumn("created_on",  $"created_on".cast(TimestampType))
      .withColumn("updated_on",  $"updated_on".cast(TimestampType))
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_hms_temporaryorder/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_hms_temporaryorder/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    /**
     * Writing data for janio_order_orderconsigneedetails table
     */
    val janio_order_orderconsigneedetails= df.filter("table='janio_order_orderconsigneedetails'")
      .withColumn("value", from_json(col("value"),
        JanioBacknedSchema.janioOrderOrderconsigneedetails)).select("value.*","event_type","event_timestamp","j_date","lsn")
      .repartition(1)
      .writeStream
      .outputMode("append")
      .option("path", config.getString("S3_WRITE_PATH")+"janio_order_orderconsigneedetails/")
      .option("checkpointLocation", config.getString("S3_KAFKA_CHECKPOINT")+"janio_order_orderconsigneedetails/")
      .option("orc.compress", "zlib")
      .format("orc")
      .partitionBy("j_date")
      .trigger(Trigger.ProcessingTime(config.getString("SCHEDULE_INTERVAL")))
      .start()

    janio_order_order.awaitTermination()
    janio_tracker_update.awaitTermination()
    janio_order_agentapplication.awaitTermination()
    janio_tracker_privateupdate.awaitTermination()
    janio_order_service.awaitTermination()
    janio_payment_paymentsettings.awaitTermination()
    janio_payment_paymentsettings_pricing_groups.awaitTermination()
    janio_payment_pricinggroup.awaitTermination()
    janio_partner_partner.awaitTermination()
    janio_order_item.awaitTermination()
    janio_order_orderpickupdetails.awaitTermination()
    janio_analytics_ordertrackerinfoanalytics.awaitTermination()
    janio_sla_orderslazone.awaitTermination()
    janio_location_zone.awaitTermination()
    janio_order_orderzone.awaitTermination()
    janio_cod_orderremittancedata.awaitTermination()
    janio_order_externalorderinfo.awaitTermination()
    janio_tracker_publicupdatev2.awaitTermination()
    janio_sla_partnerdelay.awaitTermination()
    janio_order_orderpickupdatelog.awaitTermination()
    janio_order_weightreconrecord.awaitTermination()
    janio_tracker_partnerupdate.awaitTermination()
    janio_activity_orderactivity.awaitTermination()
    janio_tracker_privateupdatev1v2link.awaitTermination()
    janio_tracker_privateupdatev2.awaitTermination()
    janio_midmile_tracker_masterairwaybill_orders.awaitTermination()
    janio_midmile_tracker_houseairwaybill_orders.awaitTermination()
    janio_hms_session.awaitTermination()
    janio_hms_sessionorder.awaitTermination()
    janio_hms_sessionorderlog.awaitTermination()
    janio_order_orderlink.awaitTermination()
    janio_hms_temporarygroup.awaitTermination()
    janio_hms_temporarygrouporders.awaitTermination()
    janio_hms_temporaryorder.awaitTermination()
    janio_order_orderconsigneedetails.awaitTermination()
  }

}
