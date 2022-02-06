package com.zdautomotive.cloud.models.rule

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

class CanTraceModelMySQL extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
        CANTools.getCANData("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas1", sc, spark)
//    CANTools.getCANData("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canTrace", sc, spark)
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    CANTools.parseCANData(dbcDF, traceDF)
  }

  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import org.apache.spark.sql.functions._
    import resultDF.sparkSession.implicits._
    //    resultDF.show(100, false)

    resultDF.filter($"id" === "1157").show(100, false)
//    val idArray = resultDF.select("id").distinct.collect.flatMap(_.toSeq)
//    val byIdArray = idArray.map(id => (id, resultDF.where($"id" <=> id)))
//
////    byIdArray.foreach(element => Future {
////      val id = element._1
////      val byIdDF = element._2
////
////      val table = dbcDF.select("botschaft").where($"identifier" <=> id).first().getString(0)
////      val keys = byIdDF.select("map").first().getMap[String, Int](0).map(_._1)
////
////      var tmpDF = byIdDF.drop("map", "id")
////      for (elem <- keys) {
////        tmpDF = tmpDF.withColumn(elem, col("map").getItem(elem))
////      }
////
////      tmpDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
////        .write.mode(SaveMode.Append)
////        .format("jdbc")
////        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
////        .option("url", "jdbc:clickhouse://192.168.0.42:9500/can")
////        .option("user", "default")
////        .option("password", "")
////        .option("dbtable", table)
////        .option("batchsize", 50000)
////        .option("isolationLevel", "NONE")
////        .save
////    }.onComplete{
////      case Success(value) => log.info(element._1 + " success " )
////      case Failure(exception) => log.info(element._1 + " failure " + exception)
////    })
//
//        for ((id, byIdDF) <- byIdArray.par) {
//          val table = dbcDF.select("botschaft").where($"identifier" <=> id).first().getString(0)
//    //      println("botschaft: " + botschaft)
//          val keys = byIdDF.select("map").first().getMap[String, Int](0).map(_._1)
//          //      val fields = row.getMap(0)
//          //      println(keys)
//          //      val map = row..getValuesMap(row.schema.fieldNames)
//          //      println(map)
//          var tmpDF = byIdDF
//          for (elem <- keys) {
//            tmpDF = tmpDF.withColumn(elem, col("map").getItem(elem))
//          }
//          //      byIdDF.show(2, false)
//    //      tmpDF.drop("map", "id")
//
//          tmpDF.drop("map", "id")
//            .write.format("jdbc")
//            .option("url", "jdbc:mysql://192.168.0.42:3310/can_q8_info?rewriteBatchedStatements=true&" + "useUnicode=true&characterEncoding=UTF-8&useSSL=false")
//            .option("driver", "com.mysql.jdbc.Driver")
//            .option("dbtable", table)
//            .option("user", "root")
//            .option("password", "123456")
//            .mode(SaveMode.Append)
//            .save()
//        }
  }
}

object CanTraceModelMySQL {
  def main(args: Array[String]): Unit = {
    val model = new CanTraceModelMySQL
    model.executeModel()
  }
}

