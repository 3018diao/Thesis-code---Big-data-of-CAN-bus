package com.zdautomotive.cloud.utils

import com.zdautomotive.cloud.config.ModelConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def createSparkSession(clazz: Class[_]): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(clazz.getSimpleName.stripSuffix("$"))

    println("------------")
    for ((key, value) <- sparkConf.getAll if key.startsWith("spark.hadoop")){
      println(key + ": " + value)
    }

    if (ModelConfig.APP_LOCAL_MODE) {
      sparkConf
        .setMaster(ModelConfig.APP_SPARK_MASTER)
    }

    val builder = SparkSession
      .builder()
      .config(sparkConf)

    val session = builder.getOrCreate()
    session
  }

  def createSparkContext(clazz: Class[_]): SparkContext ={
    val sparkConf = new SparkConf()
      .setAppName(clazz.getSimpleName.stripSuffix("$"))

    if (ModelConfig.APP_LOCAL_MODE) {
      sparkConf
        .setMaster(ModelConfig.APP_SPARK_MASTER)
    }

    val context = new SparkContext(sparkConf)
    context
  }

}
