package com.zdautomotive.cloud.etl

case class DataCAN(
                    module: Short,
                    ts: Long,
                    id: Int,
                    dir: String,
                    len: Short,
                    data: String
                  )

case class DataCAN_1(
                    module: Short,
                    ts: Long,
                    id: Int,
                    data: Array[Int]
                  )

