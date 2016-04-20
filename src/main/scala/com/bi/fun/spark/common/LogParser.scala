package com.bi.fun.spark.common

import scala.collection.mutable.Map
import scala.tools.scalap.scalax.util.StringUtil
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

/**
 * @author jeremy
 */
object LogParser {
  val family = Bytes.toBytes("f")
  
  def parse(record: String, table: HTableInterface) = {
    var logitem = formatLog(record)
    val msg = logitem.messageId.replace("mid:", "")
    if (!logitem.check) {
      if(!"f_realtime_push_reach_analyse".equals(table.getName.getNameAsString) ||  ("f_realtime_push_reach_analyse".equals(table.getName.getNameAsString) && logitem.breach)) {
    	  val inc1 = new Increment(Bytes.toBytes(logitem.platId + "_" + logitem.dateId + "_" + msg));
    	  val inc2 = new Increment(Bytes.toBytes("-1000_" + logitem.dateId + "_" + msg));
    	  val inc3 = new Increment(Bytes.toBytes(logitem.platId + "_" + logitem.dateId + "_-1000"));
    	  val inc4 = new Increment(Bytes.toBytes("-1000_" + logitem.dateId + "_-1000"));
    	  inc1.addColumn(family, Bytes.toBytes(logitem.hourId + "h"), 1);
    	  inc2.addColumn(family, Bytes.toBytes(logitem.hourId + "h"), 1);
    	  inc3.addColumn(family, Bytes.toBytes(logitem.hourId + "h"), 1);
    	  inc4.addColumn(family, Bytes.toBytes(logitem.hourId + "h"), 1);
    	  table.increment(inc1);
    	  table.increment(inc2);
    	  table.increment(inc3);
    	  table.increment(inc4);
      }
    }
  }

  def formatLog(record: String) = {
    var param = Map[String, String]()
    val data = record.split("\t")
    val url = data(2).split("\\?")(1).split("\\&")

    var dateid = CommonUtil.parseLogTimeToStr(data(1), "dd/MMM/yyyy:HH:mm:ss Z", "yyyyMMdd")
    var hourid = CommonUtil.parseLogTimeToStr(data(1), "dd/MMM/yyyy:HH:mm:ss Z", "HH")
    var msgid = ""
    var platid = ""
    var ok = ""

    if (url.exists(_.startsWith("messageid"))) {
      var msgInfo = url.filter(x => x.startsWith("messageid") && x.length() > ("messageid".length() + 1))
      if(msgInfo.length > 0) msgid = msgInfo(0).substring("messageid".length() + 1)
    }

    if (url.exists(_.startsWith("apptype"))) {
      var apptype = url.filter(_.startsWith("apptype"))
      if ("unknown".equals(apptype(0).split("=").last)) {
        var dev = url.filter(_.startsWith("dev"))
        platid = CacheUtil.getPlatId(CommonUtil.getPlatType(dev(0).substring("dev".length() + 1))).toString();
      } else {
        platid = CacheUtil.getPlatId(CommonUtil.getPlatType(apptype(0).substring("apptype".length() + 1))).toString()
      }
    } else {
      var dev = url.filter(_.startsWith("dev"))
      platid = CacheUtil.getPlatId(CommonUtil.getPlatType(dev(0).substring("dev".length() + 1))).toString()
    }

    if (url.exists(_.startsWith("ok"))) {
      ok = url.filter(_.startsWith("ok"))(0).substring("ok".length() + 1)
    }
    var logitem = new LogItem(platid, dateid, msgid, hourid, ok)
    logitem
  }

  def main(args: Array[String]): Unit = {
    val log = LogParser
    //    val ret = log.parse("23.32332    08/Mar/2016:17:31:12 +0800    /ecom_mobile/pushclick?dev=iPad_8.4.1_iPad4.2&mac=020000000000&ver=2.0.12.2&nt=1&fudid=8283F1833AAD39AD3BAD3BADD4EF36FE5998599F5DCF099B039C59CB02C80A9C&sid=0&messageid=10056&videotype=1&action=1&apptype=ipad_app_main&rprotocol=");
  }
}