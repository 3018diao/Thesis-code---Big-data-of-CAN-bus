package com.zdautomotive.cloud.utils

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import com.zdautomotive.cloud.utils.CKDataSourceSingleton.instance

object MYSQLDataSourceSingleton {
  @transient private var instance: HikariDataSource = _

  def getDataSourceInstance: HikariDataSource = {
    if (instance == null) {
      try {
        val config = new HikariConfig
        config.setJdbcUrl(
          "jdbc:mysql://master:3306/audi_a8_test?" +
            "rewriteBatchedStatements=true&useSSL=false&serverTimezone=UTC")
        config.setUsername("root")
        config.setPassword("123456")
        config.setMinimumIdle(5); //minimum number of idle connection
        config.setMaximumPoolSize(5); //maximum number of connection in the pool
        config.setConnectionTimeout(300000) //maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0); // maximum life time for each connection
        config.setIdleTimeout(0); // max idle time for recycle idle connection
        instance = new HikariDataSource(config)
      } catch {
        case ex: Exception => println(ex)
      }
    }
    instance
  }
}


