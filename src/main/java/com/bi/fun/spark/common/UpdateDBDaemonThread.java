package com.bi.fun.spark.common;


import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jeremy 同步配置信息线程
 */
public class UpdateDBDaemonThread extends Thread implements Serializable {
	private static final long serialVersionUID = 7318365333293621138L;

	public static final Log log = LogFactory.getLog(UpdateDBDaemonThread.class);
	@Override
	public void run() {
		long start = System.currentTimeMillis();
		log.info("守护线程启动成功");
		CacheUtil.initAreaList();
		log.info("首次启动，区域信息配置成功！！！！！！！！！！");
		CacheUtil.initPlatMap();
		log.info("首次启动，业务平台信息配置成功！！！！！！！！！！");
		long end = System.currentTimeMillis();
		log.info("第一次全部初始化完成，耗时：" + (end - start) + " ms.");
		while (true) {
			try {
				Thread.sleep(30 * 60 * 1000);
				CacheUtil.initAreaList();
				CacheUtil.initPlatMap();
				long lastExeTimeStamp = System.currentTimeMillis();
				log.info("-------1分钟频率加载数据库配置，耗时：" + (lastExeTimeStamp - start) + " ms.--------------------");
			} catch (InterruptedException e) {
				log.error("守护线程异常", e);
			}
		}
	}
}
