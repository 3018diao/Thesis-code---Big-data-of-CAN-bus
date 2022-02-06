package com.zdautomotive.cloud.models.rule

import java.sql.SQLException
import java.util.Date

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools, DataSourceSingleton}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class CanTraceHexToTD_2 extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    //    dbcDF.show(5,false)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    //        val frame = CANTools.getHexTrace("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canRawTrace", sc, spark)
    //    val frame = CANTools.getHexTrace("/user/huiyu.liu/canRawTrace", sc, spark)
    val frame = CANTools.getHexTrace("file:///C:\\material\\testHbase\\datas3", sc, spark)
    //    frame.show(100, false)
    frame

  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    //    println(traceDF.count())
    val frame = CANTools.parseRawCan(dbcDF, traceDF)
    //    frame.show(100,false)

    //    println("traceDF " + frame.rdd.getNumPartitions)
    //    frame.show(100,false)
    //        frame.show(100, false)
    frame
  }

  //  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
  //    import resultDF.sparkSession.implicits._
  //    resultDF.filter($"ts"==="1601253345070147")
  //      .show(5, false)
  //  }

  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import resultDF.sparkSession.implicits._

    //      dbcDF.show(100,false)
    //    resultDF.show(100, false)
    println("Parse finish. Start writing!")
    //    println(resultDF.rdd.getNumPartitions).
    val dbcMap = dbcDF.select("identifier", "botschaft")
      .dropDuplicates("identifier").orderBy("identifier")
      .map(ele => {
        Integer.parseInt(ele.get(0).toString) -> ele.get(1).toString
      }).collect().toMap

    resultDF.foreachPartition((partition: Iterator[Row]) => {
      val dsPool = DataSourceSingleton.getDataSourceInstance
      val partitionConn = dsPool.getConnection()
      val partitionStmt = partitionConn.createStatement()

      var num = 1

      val sql = "INSERT INTO "
      var sqlExecute = new StringBuilder(sql)
      var tmpId: Int = 0

      partition.foreach {
        row =>
          if (num % 5000 == 0) {
            //            println("sqlExecute: " + sqlExecute)
            //            println(num)
            try {
              partitionStmt.executeUpdate(sqlExecute.toString)
            } catch {
              case e: SQLException => {

                println(num)
                println(sqlExecute)
                e.printStackTrace
              }
            } finally {
              sqlExecute = new StringBuilder(sql)
              tmpId = 0
            }
          }

          val id = row.getAs[Int]("id")
          if (tmpId != id) {
            tmpId = id
            val table = dbcMap(id)
            sqlExecute.append("Datalogger1_" + table + " USING " + table + " TAGS (2,\"Datalogger1\")" + " VALUES ")
          }
          val ts = row.getAs[Long]("ts")

          val dataArray = row.getSeq[Long](2)

          sqlExecute.append("(" + ts.toString + ",")
          dataArray.foreach(sqlExecute.append(_).append(","))
          sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") ")

          num += 1
      }
      //            println(sqlExecute.toString)
      println("-------------------------")
      partitionStmt.executeUpdate(sqlExecute.toString)
      //      partitionStmt.close()
      //      partitionConn.close()
      //      dsPool.close()
      //      dsPool.evictConnection(partitionConn)
      println(num)
      partitionConn.close()
    })
  }

}

object CanTraceHexToTD_2 {
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val model = new CanTraceHexToTD_2
    model.executeModel()
    var end_time = new Date().getTime
    println(end_time - start_time)
  }
}



