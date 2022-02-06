package com.zdautomotive.cloud.etl

case class DataCAN_Date(module: Short,
                        ts: String,
                        id: Int,
                        dir: String,
                        len: Short,
                        data: String)
