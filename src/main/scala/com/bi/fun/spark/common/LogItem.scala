package com.bi.fun.spark.common

/**
 * @author jeremy
 */
class LogItem(val platId: String, val dateId: String, val messageId: String, val hourId: String, ok: String) {
  def check = platId.length() == 0 || dateId.length() == 0 || messageId.length() == 0 || hourId.length() == 0
  def breach = "1".equals(ok)
}