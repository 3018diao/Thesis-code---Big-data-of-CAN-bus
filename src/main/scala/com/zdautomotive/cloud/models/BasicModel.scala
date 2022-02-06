package com.zdautomotive.cloud.models

import com.zdautomotive.cloud.utils.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

trait BasicModel extends Logging {

  var spark: SparkSession = _
  var sc: SparkContext = _

  def init() = {
    sc = SparkUtils.createSparkContext(this.getClass)
    spark = SparkUtils.createSparkSession(this.getClass)
  }

  def getDBC(): DataFrame

  def getTraceData(): DataFrame

  def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame

  def saveResult(resultDF: DataFrame, dbcDF: DataFrame = null)

  def close() = {
    if (null != sc) sc.stop()
    if (null != spark) spark.stop()
  }

  def executeModel() = {
    // a. Init Spark
    init()
    try {
      // b. Get DBC data
      val dbcDF = getDBC()
      dbcDF.persist(StorageLevel.MEMORY_AND_DISK).count()
      // c. Get Trace data
      val traceDF = getTraceData()
      // d. Parse trace according to DBC
      val resultDF = parseTrace(dbcDF, traceDF)
      // e. Write result to database
      saveResult(resultDF, dbcDF)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // f. close Spark
      close()
    }
  }
}
