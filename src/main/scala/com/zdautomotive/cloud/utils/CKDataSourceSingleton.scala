package com.zdautomotive.cloud.utils

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object CKDataSourceSingleton {
  @transient private var instance: HikariDataSource = _
  //  @transient private var instance: DruidDataSource = _

  def getDataSourceInstance: HikariDataSource = {
    if (instance == null) {
      try {
        val config = new HikariConfig
        config.setDriverClassName("com.github.housepower.jdbc.ClickHouseDriver")
        config.setJdbcUrl("jdbc:clickhouse://master:9000/audi_a8")

        config.setUsername("default")
        config.setPassword("")
        config.setMinimumIdle(10);           //minimum number of idle connection
        config.setMaximumPoolSize(15);      //maximum number of connection in the pool
        config.setConnectionTimeout(300000000) //maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0);       // maximum life time for each connection
        config.setIdleTimeout(0);       // max idle time for recycle idle connection
        instance = new HikariDataSource(config)
      } catch {
        case ex: Exception => println(ex)
      }
    }
    instance
  }
}
