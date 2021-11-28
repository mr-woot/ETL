package com.janio.etl

import com.janio.connection.SparkInstance
import org.apache.spark.sql.{DataFrame, SaveMode}

object Read {

  def main(args: Array[String]): Unit = {
    val spark = SparkInstance.getSparkInstance()
    import spark.implicits._
    //      val df= spark.read.orc("s3a://janio-data-lake/janio-internal-db/janiobackned/prod-data/janio_order_orderpickupdetails/")
    //      df.orderBy(asc("id")).show(false)
   /* val df: DataFrame = spark.read
        .format("jdbc")
      .option("driver","com.amazon.redshift.jdbc42.Driver")
      .option("url", "jdbc:redshift://redshift-data-bi.ctfgkjxae9os.ap-southeast-1.redshift.amazonaws.com:5439/warehouse")
      .option("user","janioadmin")
      .option("password","Janio-admin-123")
      .option("dbtable", "test")
      .option("tempdir", "s3a://janio-data-lake/redshift-temp/")
      .load()
    df.show()*/

    val df= spark.read.csv("/Users/rahul/Downloads/input/*.csv")

    df
      .write
      .format("jdbc")
      .option("driver","com.amazon.redshift.jdbc42.Driver")
      .option("url", "jdbc:redshift://redshift-data-bi.ctfgkjxae9os.ap-southeast-1.redshift.amazonaws.com:5439/warehouse")
      .option("user","janioadmin")
      .option("password","Janio-admin-123")
      .option("tempdir", "s3a://janio-data-lake/redshift-temp/")
      .option("dbtable", "test")
      .mode(SaveMode.Overwrite) // <--- Append to the existing table
      .save()


  }

}

