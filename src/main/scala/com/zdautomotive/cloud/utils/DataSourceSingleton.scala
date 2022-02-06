package com.zdautomotive.cloud.utils

import com.alibaba.druid.pool.DruidDataSource
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object DataSourceSingleton {
  @transient private var instance: HikariDataSource = _

  def getDataSourceInstance: HikariDataSource = {
    if (instance == null) {
      try {
        val config = new HikariConfig
        config.setJdbcUrl("jdbc:TAOS://master:6030/audi_a8")
        config.setUsername("root")
        config.setPassword("taosdata")
        config.setMinimumIdle(10);           //minimum number of idle connection
        config.setMaximumPoolSize(15);      //maximum number of connection in the pool
        config.setConnectionTimeout(300000) //maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0);       // maximum life time for each connection
        config.setIdleTimeout(0);       // max idle time for recycle idle connection
        config.setConnectionTestQuery("select server_status()")
        instance = new HikariDataSource(config)
      } catch {
        case ex: Exception => println("fuck!!!!!!!!!!!!!!!!!!!!!!!!")
      }
    }
    instance
  }

}
