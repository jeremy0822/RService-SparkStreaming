package com.bi.fun.spark.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jeremy
 * @date 2016年2月29日
 * @desc
 */
public class CacheUtil {
	private static final Log log = LogFactory.getLog(CacheUtil.class);

	public static UpdateDBDaemonThread daemon = new UpdateDBDaemonThread();

	public static List<DmArea> areaList = new ArrayList<DmArea>();
	public static Map<String, Integer> platMap = new HashMap<String, Integer>();
	
	static {
		daemon.start();
	}

	/**
	 * 初始化地域配置信息
	 * 
	 */
	public static void initAreaList() {
		List<DmArea> temList = new ArrayList<DmArea>();
		String sql = "SELECT t1.iplong_start,t1.iplong_end,t1.area_id as province_id,t2.area_name as province_name,0 as country_id,'' as country_name FROM t_iplib t1 LEFT JOIN dm_area t2 ON t1.area_id = t2.area_id AND t2.country_id = 0 AND t2.second_area_id = 0 WHERE t1.area_id < 100000 UNION SELECT t1.iplong_start,t1.iplong_end,0 as province_id,'' as province_name, t1.area_id as country_id,t2.area_name as country_name FROM t_iplib t1 LEFT JOIN dm_area t2 ON t1.area_id = t2.country_id AND t2.area_id = 0 WHERE t1.area_id >= 100000";
		ResultSet rs = null;
		Connection con = null;
		try {
			con = DbUtil.getConnection();
			rs = DbUtil.executeQuery(con, sql);
			if (rs != null) {
				while (rs.next()) {
					DmArea dmArea = new DmArea();
					dmArea.setStartIP(rs.getLong("iplong_start"));
					dmArea.setEndIP(rs.getLong("iplong_end"));
					dmArea.setProvinceID(rs.getString("province_id"));
					dmArea.setProvinceName(rs.getString("province_name"));
					dmArea.setCountyID(rs.getString("country_id"));
					dmArea.setCountyName(rs.getString("country_name"));
					temList.add(dmArea);
				}
			}
			log.info("areaList.size():" + temList.size());
			if (!areaList.isEmpty()) {
				areaList.clear();
			}
			areaList.addAll(temList);
			temList = null;
		} catch (Exception e) {
			log.error("地域配置缓存异常", e);
		} finally {
			DbUtil.close(rs);
			DbUtil.close(con);
		}
	}

	/**
	 * 初始化平台配置信息
	 * 
	 */
	public static void initPlatMap() {
		Map<String, Integer> tempMap = new HashMap<String, Integer>();
		String sql = "SELECT plat_id,plat_name from dm_plat";
		ResultSet rs = null;
		Connection con = null;
		try {
			con = DbUtil.getConnection();
			rs = DbUtil.executeQuery(con, sql);
			if (rs != null) {
				while (rs.next()) {
					tempMap.put(rs.getString("plat_name"), rs.getInt("plat_id"));
				}
			}
			log.info("platMap.size():" + tempMap.size());
			if (!platMap.isEmpty()) {
				platMap.clear();
			}
			platMap.putAll(tempMap);
			tempMap = null;
		} catch (Exception e) {
			log.error("平台维度配置缓存异常", e);
		} finally {
			DbUtil.close(rs);
			DbUtil.close(con);
		}
	}

	/**
	 * 根据IP获取区域
	 * 
	 * @param ip
	 * @return
	 */
	public static DmArea getArea(Long ip) {
		DmArea dm = null;
		try {
			if (areaList != null && !areaList.isEmpty()) {
				for (DmArea dmArea : areaList) {
					if (ip >= dmArea.getStartIP() && ip <= dmArea.getEndIP()) {
						dm = dmArea;
					}
				}
			}
		} catch (Exception e) {
			log.info("获取区域异常：", e);
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
		return dm;
	}

	public static Integer getPlatId(String platInfo) {
		if (platMap != null && !platMap.isEmpty()) {
			return platMap.get(platInfo);
		}
		return 99;
	}

	public static long ip2long(String ipStr) {

		String retVal = "0";
		long[] nFields = new long[4];

		try {
			ipStr = ipFormat(ipStr);
			String[] strFields = ipStr.split("\\.");
			for (int i = 0; i < 4; i++) {
				nFields[i] = Integer.parseInt(strFields[i]);
			}

			retVal = String.format("%s", (nFields[0] << 24)

					| (nFields[1] << 16) | (nFields[2] << 8) | nFields[3]);
		} catch (Exception e) {
			log.error("ip格式不对:" + e.getMessage(), e.getCause());
		}

		return Long.parseLong(retVal);
	}

	public static String ipFormat(String ipStr) {

		Pattern pattern = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
		Matcher matcher = pattern.matcher(ipStr);

		if (ipStr == null || "".equalsIgnoreCase(ipStr.trim()) || !(matcher.matches())) {

			ipStr = "0.0.0.0";
		}

		return ipStr;
	}

	/**
	 * 启动本进程中配置更新守护线程
	 * 
	 */
	public static void daemonStart() {
		try {
			// 启动守护线程
			if (!daemon.isAlive()) {
				Thread.sleep((int) (Math.random() * 1000));
				if (!daemon.isAlive()) {
					daemon.start();
				}
			}
		} catch (Exception e) {
			log.info("线程已启动", e);
		}
	}
}
