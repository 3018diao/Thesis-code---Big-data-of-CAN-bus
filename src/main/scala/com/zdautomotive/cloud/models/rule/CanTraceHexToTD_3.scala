package com.zdautomotive.cloud.models.rule

import java.sql.SQLException
import java.util.Date

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools, DataSourceSingleton}
import org.apache.spark.sql.{DataFrame, Row}

class CanTraceHexToTD_3 extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    //    dbcDF.show(5,false)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
//    CANTools.getHexTrace_clone("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canRawTrace", sc, spark)

    val frame = CANTools.getHexTrace_clone("file:///C:\\material\\testHbase\\datas3", sc, spark)
    frame
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    import traceDF.sparkSession.implicits._
    val frame = CANTools.parseRawCan(dbcDF, traceDF)
    frame.filter($"identifier" === 168)
  }

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

    println(resultDF.count())


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
          //          println("start")
          if (num % 5000 == 0) {
            //            println("sqlExecute: " + sqlExecute)
            println(num)
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
            sqlExecute.append("Datalogger2_" + table + " USING " + table + " TAGS (2,\"Datalogger2\")" + " VALUES ")
          }
          val ts = row.getAs[Long]("ts")

          val dataArray = row.getSeq[Long](2)

          sqlExecute.append("(" + ts.toString + ",")
          dataArray.foreach(sqlExecute.append(_).append(","))
          sqlExecute.deleteCharAt(sqlExecute.length - 1).append(") ")

          num += 1
      }
      //      println(sqlExecute.toString)
      println("-------------------------")
      partitionStmt.executeUpdate(sqlExecute.toString)
      //      partitionStmt.close()
      //      partitionConn.close()
      //      dsPool.close()
      //      dsPool.evictConnection(partitionConn)
      println(num)
      partitionConn.close()
      println("end")
    })
  }
}

object CanTraceHexToTD_3 {
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    println("---------------------")
    val model = new CanTraceHexToTD_3
    model.executeModel()
    var end_time = new Date().getTime
    println(end_time - start_time)
  }
}
