package com.zdautomotive.cloud.etl

case class DataTrace(
                      canid: Int,
                      cs: DataCS,
                      data: Array[Byte]
                    )