package com.zdautomotive.cloud.models.rule

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}

class CanTraceHexToCK  extends BasicModel {
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
//    dbcDF.show(5,false)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    val frame = CANTools.getHexTrace("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canRawTrace", sc, spark)
//    val frame = CANTools.getHexTrace("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas3", sc, spark)
    frame

  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
//    println(traceDF.count())
    val frame = CANTools.parseRawCan(dbcDF, traceDF)
//    frame.show(100,false)

    //    println("traceDF " + frame.rdd.getNumPartitions)
//    frame.show(100,false)
    frame
  }

//  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
//    resultDF.show(5, false)
//  }


  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import resultDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //    resultDF.show(100, false)
    val dbcMap = dbcDF.select("identifier", "botschaft")
      .dropDuplicates("identifier").orderBy("identifier")
      .map(ele => {
        Integer.parseInt(ele.get(0).toString) -> ele.get(1).toString
      }).collect().toMap

//    resultDF.groupBy("id").agg(sum(""))
//    println(resultDF.count())

    val value = resultDF.repartition($"id")
    println(value.rdd.getNumPartitions)
    value.foreachPartition((partition: Iterator[Row]) => {
      //      println("---------------------> Partition start ")
      val partitionID = TaskContext.getPartitionId()
      //      println("partitionID is "+TaskContext.getPartitionId())
      if (partition.hasNext) {
        val tmpRow = partition.next()
        val id = tmpRow.getAs[Int]("id")
        val table = dbcMap(id)
        val ts = tmpRow.getAs[Long]("ts")

        val colums = tmpRow.getAs[Map[String, Long]]("map").keys
        val values = tmpRow.getAs[Map[String, Long]]("map").values

        Class.forName("com.github.housepower.jdbc.ClickHouseDriver")
        var conn: Connection = null
        var pstmt: PreparedStatement = null
        try {
          conn = DriverManager.getConnection(
            "jdbc:clickhouse://192.168.0.42:9500/can_q8_info",
            "default",
            ""
          )

          val sql = new StringBuilder("insert into can_q8_info.").append(table + " (ts,")
          colums.foreach(column => sql.append(column + ","))
          sql.deleteCharAt(sql.length - 1).append(") values (?,")
          colums.foreach(_ => sql.append("?,"))
          sql.deleteCharAt(sql.length - 1).append(")")
          pstmt = conn.prepareStatement(sql.toString)
          //          println(partitionID + " " + sql)

          pstmt.setLong(1, ts)
          values.zip(0 until values.size).foreach { case (value, index) =>
            pstmt.setLong(index + 2, value)
          }
          pstmt.addBatch()

          var i = 0
          partition.foreach {
            row =>
              val rowTs = row.getAs[Long]("ts")
              val rowValues = row.getAs[Map[String, Long]]("map").values
              pstmt.setLong(1, rowTs)

              rowValues.zip(0 until values.size).foreach { case (value, index) =>
                pstmt.setLong(index + 2, value)
              }
              pstmt.addBatch()
              i = i + 1
              if (i % 50000 == 0) {
                pstmt.executeBatch()
              }
          }
          pstmt.executeBatch()

        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (null != pstmt) pstmt.close()
          if (null != conn) conn.close()
        }
        //    })

        //      try {
        //        conn = DriverManager.getConnection(
        //          "jdbc:clickhouse://192.168.0.42:8123/can",
        //          "default",
        //          ""
        //        )
        //        stmt = conn.createStatement()
        //        var i = 0
        //        partition.foreach {
        //          row => {
        ////            println("datas~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        //            //            var sql = "insert into can."
        //            // Get table name
        //            val sql = new StringBuilder("insert into can.")
        //            val id = row.getAs[Int]("id")
        //            val table = dbcMap(id)
        //            val ts = row.getAs[Long]("ts")
        //            val dataMap = row.getAs[Map[String, Long]]("map")
        //            sql.append(table + " (ts,")
        //            dataMap.keys.foreach(key => sql.append(key + ","))
        //            sql.deleteCharAt(sql.length - 1)
        //            sql.append(") values (" + ts.toString + ",")
        //            dataMap.values.foreach(value => sql.append(value + ","))
        //            sql.deleteCharAt(sql.length - 1)
        //            sql.append(")")
        //            i = i + 1
        ////            println(i)
        ////            stmt.addBatch(sql.toString)
        ////            println(i)
        //            if (i % 50 == 0) {
        ////              stmt.executeBatch()
        ////              stmt.clearBatch()
        //              println(i)
        //            }
        //          }
        //        }
        ////        stmt.executeBatch()
        //        ()
        //      } catch {
        //        case e: Exception => e.printStackTrace()
        //      } finally {
        ////        if (null != stmt) stmt.close()
        ////        if (null != conn) conn.close()
        //      }
        //    })

        println("----------------")
        //        partition.foreach{
        //          row => {
        //            row.getInt("ts")
        //          }
        //        }
      }

    })
  }

}

object CanTraceHexToCK {
  def main(args: Array[String]): Unit = {
    val model = new CanTraceHexToCK
    model.executeModel()
  }
}
