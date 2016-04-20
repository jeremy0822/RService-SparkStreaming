package com.bi.fun.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import com.bi.fun.spark.common.Constant
import com.bi.fun.spark.common.HbaseConnectionPool
import com.bi.fun.spark.common.LogParser
import org.apache.spark.storage.StorageLevel
import com.bi.fun.spark.common.LogParser
/**
 * @author jeremy
 */
object PushReachStat extends App {

  if (args.length < 3) {
    System.err.println("Only" + args.length + " arguments supplied,required:3")
    System.err.println("Usage: ./bin/start <kafka.broker.list> <topics><hbase table>")
    System.exit(1)
  }
  val Array(brokers, topics,hbaseTable) = args
  val kafkaParams = scala.collection.immutable.Map(
    "zookeeper.connect" -> brokers,
    "zookeeper.connection.timeout.ms" -> "30000",
    "group.id" -> Constant.kafkaGroup)

  //通过spark－submit方式提交，通过spark参数传递
  val conf = new SparkConf().setAppName(hbaseTable).setMaster("local[4]")
  val ssc = new StreamingContext(conf, Seconds(30))
//  ssc.checkpoint("checkpoints")
    
  val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map(topics -> 2),StorageLevel.MEMORY_AND_DISK_SER)
  val messages = kafkaStream.window(Seconds(30))
  //处理消息入库
  messages.foreachRDD(rdd => {
    rdd.foreachPartition(partition => {
      val pool = HbaseConnectionPool.getTable(hbaseTable)
      partition.foreach(record=>LogParser.parse(record._2, pool))
      HbaseConnectionPool.close(pool)
    })
  })
  ssc.start()
  ssc.awaitTermination()
}