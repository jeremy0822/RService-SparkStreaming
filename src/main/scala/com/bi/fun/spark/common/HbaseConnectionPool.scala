package com.bi.fun.spark.common

import java.io.IOException
import org.apache.hadoop.hbase.client.{ HConnection, HConnectionManager }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTableInterface

/**
 * @author jeremy
 */
object HbaseConnectionPool {

  private var pool: HConnection = _

  def getHConnection = {
    if (pool == null) {
      val config = HBaseConfiguration.create();
      pool = HConnectionManager.createConnection(config);
    }
    pool
  }

  def getTable(tableName: String) = {
    var table = getHConnection.getTable(tableName)
    table.setAutoFlush(false, true);
    table
  }

  def close(table: HTableInterface) = {
    table.flushCommits()
    table.close()
  }
}