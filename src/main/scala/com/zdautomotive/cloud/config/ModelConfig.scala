package com.zdautomotive.cloud.config

import com.typesafe.config.{Config, ConfigFactory}

object ModelConfig {

  // load config.properties
  private val config: Config = ConfigFactory.load("config.properties")

  lazy val APP_LOCAL_MODE = config.getBoolean("app.is.local")
  lazy val APP_SPARK_MASTER = config.getString("app.spark.master")

  lazy val MYSQL_JDBC_DRIVER = config.getString("mysql.jdbc.driver")
  lazy val MYSQL_JDBC_URL = config.getString("mysql.jdbc.url")
  lazy val MYSQL_JDBC_USERNAME = config.getString("mysql.jdbc.username")
  lazy val MYSQL_JDBC_PASSWORD = config.getString("mysql.jdbc.password")

  lazy val ZK_HOSTS = config.getString("hbase.zk.hosts")
  lazy val ZK_PORT = config.getString("hbase.zk.port")
  lazy val ZK_ZNODE = config.getString("hbase.zk.znode")


}
