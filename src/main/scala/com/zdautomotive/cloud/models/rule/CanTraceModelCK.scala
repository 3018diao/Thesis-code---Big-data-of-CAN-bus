package com.zdautomotive.cloud.models.rule

import java.util.Date

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

import scala.concurrent.ExecutionContext.Implicits._
import scala.util._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration.Duration.Inf
import scala.util.{Failure, Success}

class CanTraceModelCK extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    println("dbcDF partiton: "+dbcDF.rdd.getNumPartitions)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
            CANTools.getCANData("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas1", sc, spark)
//    val frame = CANTools.getCANData("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canTrace", sc, spark)
//    println(frame.rdd.getNumPartitions)
//    frame
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    val frame = CANTools.parseCANData(dbcDF, traceDF)
//    println("traceDF" + frame.rdd.getNumPartitions)
    frame.show(100, false)
    frame
  }

  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
//    import resultDF.sparkSession.implicits._
//    import org.apache.spark.sql.functions._
//    //    resultDF.show(100, false)
//    dbcDF.cache()
//    println("----------------")
//    println(resultDF.rdd.getNumPartitions)
//    println("-----------")
//    println("save starting")
////    resultDF.show(5,false)
//    val idArray = resultDF.select("id").distinct.collect.flatMap(_.toSeq)
//    val byIdArray = idArray.map(id => (id, resultDF.where($"id" <=> id)))
//    var totalNum = byIdArray.length
//    println("totalNum: " + totalNum)
//
////    val futures1 = byIdArray.map(element => ConcurrentContext.executeAsync({
////      //      dbcDF.show(1,false)
////      //      println("write starting")
////      var start_time = new Date().getTime
////      val id = element._1
////      val byIdDF = element._2
////      //      byIdDF.show(1, false)
////      println(id + " start")
////      //      println(": " + id)
////      val table = dbcDF.select("botschaft").where($"identifier" <=> id).first().getString(0)
//////      println("table: " + table)
////      val keys = byIdDF.select("map").first().getMap[String, Int](0).map(_._1)
//////      println(byIdDF.rdd.getNumPartitions)
////      var tmpDF = byIdDF.coalesce(20)
//////      println(tmpDF.rdd.getNumPartitions)
////      for (elem <- keys) {
////        tmpDF = tmpDF.withColumn(elem, col("map").getItem(elem))
////      }
////      println(table+": " + tmpDF.rdd.getNumPartitions)
////      tmpDF.drop("map", "id")
////        .persist(StorageLevel.MEMORY_ONLY)
//////                .show(1, false)
////        .write.mode(SaveMode.Append)
////        .format("jdbc")
////        //        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
////        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
////        .option("url", "jdbc:clickhouse://192.168.0.42:9500/can")
////        .option("user", "default")
////        .option("password", "")
////        .option("dbtable", table)
////        .option("batchsize", 50000)
////        .option("isolationLevel", "NONE")
////        .save
////
////      tmpDF.unpersist()
////          var end_time = new Date().getTime
////          println("table time： " +(end_time - start_time))
////      println(id + " end")
////            totalNum = totalNum - 1
////            println("Still remain: "+totalNum)
////      //      tmpDF.drop("map", "id")
////      //        .persist(StorageLevel.MEMORY_AND_DISK_SER)
////      //        //        .show(5, false)
////      //        .write
////      //        .option("header", "true")
////      //        .csv("/ck/" + table + ".csv")
////      //      println(id + " end")
////      //      println(id + " end")
////    }))
////    Thread.sleep(15000)
////    ConcurrentContext.awaitAll(futures1)
//
//
//    //    Thread.sleep(15000)
//
//    for ((id, byIdDF) <- byIdArray.par) {
//      totalNum = totalNum - 1
//      println("Still remain: "+totalNum)
//      val table = dbcDF.select("botschaft").where($"identifier" <=> id).first().getString(0)
//      //      println("botschaft: " + botschaft)
//      println(table+" start")
//      val keys = byIdDF.select("map").first().getMap[String, Int](0).map(_._1)
//      //      val fields = row.getMap(0)
//      //      println(keys)
//      //      val map = row..getValuesMap(row.schema.fieldNames)
//      //      println(map)
//      var tmpDF = byIdDF.repartition(2)
//      println("tmpDF" + tmpDF.rdd.getNumPartitions)
//      for (elem <- keys) {
//        tmpDF = tmpDF.withColumn(elem, col("map")
//          .getItem(elem))
//      }
//      //      byIdDF.show(2, false)
//      //      tmpDF.drop("map", "id")
//
//      println(table + " write")
//      tmpDF.drop("map", "id")
////        .persist(StorageLevel.MEMORY_AND_DISK_SER)
//        .write
//        .mode(SaveMode.Append)
//        .format("jdbc")
//        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
//        .option("url", "jdbc:clickhouse://192.168.0.42:9500/can")
//        .option("user", "default")
//        .option("password", "")
//        .option("dbtable", table)
//        .option("batchsize", 50000)
//        .option("isolationLevel", "NONE")
//        .save
//
//      println(table + "end")
//    }
  }
}

object CanTraceModelCK {
  def main(args: Array[String]): Unit = {
    val model = new CanTraceModelCK
    model.executeModel()
  }
}

object ConcurrentContext {

  import scala.util._
  import scala.concurrent._
  import scala.concurrent.ExecutionContext.Implicits.global

  /** Wraps a code block in a Future and returns the future */
  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  def awaitAll[T](it: Array[Future[T]], timeout: Duration = Inf) = {
    for (elem <- it) {
      Await.ready(elem, Duration.Inf)
    }
    //    Await.result(Future.sequence(it), timeout)
  }
}