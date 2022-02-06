package com.zdautomotive.cloud.models.rule

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

class CanTraceBasicModelTD extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("can_test", spark)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    CANTools.getCANData("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas1", sc, spark)
    //    CANTools.getCANData("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canTrace", sc, spark)
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    import dbcDF.sparkSession.implicits._
    val tsToDate_udf = udf {
      (ts: Long) => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
        val date = sdf.format(1623253560000L)
        date
      }
    }
    traceDF.withColumn("timestamp", tsToDate_udf('ts))
  }


  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import resultDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val moduleArray = resultDF.select("module").distinct.collect.flatMap(_.toSeq)
    val byModuleArray = moduleArray.map(module => (module, resultDF.where($"module" <=> module)))

    byModuleArray.foreach(element => {
      val module = element._1
      val byModuleDF = element._2

      byModuleDF
        .select($"timestamp".as("ts"),$"id", $"len", $"data")
//        .show(5, false)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
        .write.format("jdbc")
        .option("url", "jdbc:TAOS://192.168.0.42:6030/can?charset=UTF-8&locale=en_US.UTF-8")
        .option("driver", "com.taosdata.jdbc.TSDBDriver")
        .option("dbtable", "datalogger_1_can" + module)
        .option("user", "root")
        .mode(SaveMode.Append)
        .save()
    })
  }
}

object CanTraceBasicModelTD {
  def main(args: Array[String]): Unit = {
    val model = new CanTraceBasicModelTD
    model.executeModel
  }
}