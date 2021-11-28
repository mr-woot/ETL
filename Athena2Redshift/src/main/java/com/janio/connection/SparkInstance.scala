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
      .appName("Parsing csv files to parquet")
      .master(config.getString("SPARK_HOST"))
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate();
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.access.key", config.getString("S3_ACCESS_KEY"))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", config.getString("S3_Secret_KEY"))
    sc.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark
  }
}
