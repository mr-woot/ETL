package com.janio.connection

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkInstance {

  val infoLogger = LoggerFactory.getLogger(SparkInstance.getClass)
  val config = ConfigFactory.load("config")

  /**
    * @return SparkSession
    */
    def getSparkInstance() :SparkSession ={
    infoLogger.info("Initializing spark session")
    val spark = SparkSession
      .builder()
      .appName(config.getString("SPARK_APP_NAME"))
      .master(config.getString("SPARK_HOST"))
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate();
    val sc = spark.sparkContext
      sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      sc.hadoopConfiguration.set("fs.gs.project.id", config.getString("PROJECT_ID"))
//      sc.hadoopConfiguration.set("fs.gs.system.bucket", config.getString("BUCKET_NAME"))
      sc.hadoopConfiguration.set("fs.gs.auth.service.account.json.keyfile",
        config.getString("GOOGLE_APPLICATION_CREDENTIALS"))

    spark
  }
}
