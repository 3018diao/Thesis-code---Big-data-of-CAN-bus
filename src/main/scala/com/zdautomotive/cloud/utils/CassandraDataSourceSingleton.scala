package com.zdautomotive.cloud.utils

import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object CassandraDataSourceSingleton {
  @transient private var instance: Cluster = _
  //  @transient private var instance: DruidDataSource = _

  def getDataSourceInstance: Cluster = {
    if (instance == null) {
      try {
        val poolingOptions = new PoolingOptions
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32)
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 2)
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 4)

        val host = "127.0.0.1"

        val port = 9042
        val cluster = Cluster.builder()
          .addContactPoint(host)
          .withPort(port)
          .withPoolingOptions(poolingOptions)
          .build
        instance = cluster
      } catch {
        case ex: Exception => println(ex)
      }
    }
    instance
  }
}
