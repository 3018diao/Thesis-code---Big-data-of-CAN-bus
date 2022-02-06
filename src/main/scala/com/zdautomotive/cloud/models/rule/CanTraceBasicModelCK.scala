package com.zdautomotive.cloud.models.rule

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

class CanTraceBasicModelCK extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("can_test", spark)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    import org.apache.spark.sql.functions._

    val frame = CANTools.getCANData("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas1", sc, spark)
    import frame.sparkSession.implicits._
    frame.filter($"id"==="402522483").show(100, false)
    frame
//    val frame = CANTools.getCANData("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canTrace", sc, spark)
//    println(frame.rdd.getNumPartitions)
//    frame
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    import traceDF.sparkSession.implicits._
//    traceDF.select($"module", $"ts", $"id").sort()
    println("traceDF" + traceDF.rdd.getNumPartitions)
    traceDF
  }

  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import org.apache.spark.sql.functions._
    import resultDF.sparkSession.implicits._
    resultDF.filter($"id"==="402522483").show(100, false)
    println(resultDF.count())
//    println("partition: " + resultDF.rdd.getNumPartitions)
//    val value = resultDF.coalesce(200)
//    println(value.rdd.getNumPartitions)
//    value
//      .persist(StorageLevel.MEMORY_AND_DISK_SER)
//      .write.mode(SaveMode.Append)
//      .format("jdbc")
//      .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
//      .option("url", "jdbc:clickhouse://192.168.0.42:9500/cantest")
//      .option("user", "default")
//      .option("password", "")
//      .option("dbtable", "canTrace")
//      .option("batchsize", 50000)
//      .option("isolationLevel", "NONE")
//      .save
//
//    resultDF.unpersist()
  }
}

object CanTraceBasicModelCK {
  def main(args: Array[String]): Unit = {
    val model = new CanTraceBasicModelCK
    model.executeModel()
  }
}



