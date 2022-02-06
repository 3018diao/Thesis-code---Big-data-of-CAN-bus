package com.zdautomotive.cloud.models.rule

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zdautomotive.cloud.models.BasicModel
import com.zdautomotive.cloud.utils.{CANTools, DBCTools}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}

class CanTraceModelMySQLPartition extends BasicModel{
  override def getDBC(): DataFrame = {
    val dbcDF = DBCTools.getDBCData("MLBevo_Gen2_MLBevo_ICAN_KMatrix", spark)
    println("dbcDF partiton: " + dbcDF.rdd.getNumPartitions)
    DBCTools.aggDBC(dbcDF)
  }

  override def getTraceData(): DataFrame = {
    //    CANTools.getCANData("file:///C:\\Users\\huiyu.liu\\IdeaProjects\\testHbase\\datas", sc, spark)
    CANTools.getCANData("file:////opt/spark-3.1.1-bin-hadoop2.7/data/canTrace", sc, spark)
    //    println(frame.rdd.getNumPartitions)
    //    frame
  }

  override def parseTrace(dbcDF: DataFrame, traceDF: DataFrame): DataFrame = {
    val frame = CANTools.parseCANData(dbcDF, traceDF)
    //    println("traceDF " + frame.rdd.getNumPartitions)
    frame
  }


  override def saveResult(resultDF: DataFrame, dbcDF: DataFrame): Unit = {
    import resultDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //    resultDF.show(100, false)
    val dbcMap = dbcDF.select("identifier", "botschaft")
      .dropDuplicates("identifier").orderBy("identifier")
      .map(ele => {
        Integer.parseInt(ele.get(0).toString) -> ele.get(1).toString
      }).collect().toMap

    //    resultDF.show(5, false)

    val value = resultDF.repartition($"id")
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

        Class.forName("com.mysql.jdbc.Driver")
        var conn: Connection = null
        var pstmt: PreparedStatement = null
        try {
          conn = DriverManager.getConnection(
            "jdbc:mysql://192.168.0.42:3310/can_q8_info?rewriteBatchedStatements=true&" + "useUnicode=true&characterEncoding=UTF-8&useSSL=false",
            "root",
            "123456"
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


      //      partition.collectFirst(println)
      //      println("=====================> Partition stop ")
    })
  }
}

object CanTraceModelMySQLPartition {
  def main(args: Array[String]): Unit = {
    val model = new CanTraceModelMySQLPartition
    model.executeModel()
  }
}
