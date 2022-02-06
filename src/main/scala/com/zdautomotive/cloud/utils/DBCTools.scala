package com.zdautomotive.cloud.utils

import com.zdautomotive.cloud.config.ModelConfig
import org.apache.spark.sql.functions.{collect_list, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DBCTools {
  def getDBCData(table: String, spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", ModelConfig.MYSQL_JDBC_URL)
      .option("driver", ModelConfig.MYSQL_JDBC_DRIVER)
      .option("dbtable", table)
      .option("user", ModelConfig.MYSQL_JDBC_USERNAME)
      .option("password", ModelConfig.MYSQL_JDBC_PASSWORD)
      .load()
  }

  def aggDBC(dbcDF: DataFrame): DataFrame ={
    import dbcDF.sparkSession.implicits._
    dbcDF.select($"identifier", $"botschaft", $"signal_name", $"startBit", $"len")
      .withColumn("struct", struct("signal_name", "startBit", "len"))
      .groupBy("identifier", "botschaft")
      .agg(collect_list("struct") as "info")
      .orderBy("identifier")
  }
}
