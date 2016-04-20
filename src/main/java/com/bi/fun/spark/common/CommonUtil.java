package com.bi.fun.spark.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * @author jeremy
 * @date 2016年2月29日
 * @desc
 */
public class CommonUtil {

	public static String getPlatType(String origPlatTypeStr) {
		origPlatTypeStr = origPlatTypeStr.toLowerCase();

		if (origPlatTypeStr.equalsIgnoreCase("iphone_app_main")) {
			return "iphone";
		} else if (origPlatTypeStr.equalsIgnoreCase("aphone_app_main")) {
			return "aphone";
		} else if (origPlatTypeStr.equalsIgnoreCase("ipad_app_main")) {
			return "ipad";
		} else if (origPlatTypeStr.equalsIgnoreCase("apad_app_main")) {
			return "apad";
		} else if (origPlatTypeStr.equalsIgnoreCase("wphone_app_main")) {
			return "wphone";
		} else if (origPlatTypeStr.equalsIgnoreCase("wpad_app_main")) {
			return "wpad";
		} else if (origPlatTypeStr.equalsIgnoreCase("xm_app_media")) {
			return "SDK合作";
		} else if (origPlatTypeStr.equalsIgnoreCase("tv_app_main")) {
			return "tv";
		}
		return "other";
	}

	public static String getPlatTypeFromDev(String originDev) throws Exception {
		// 格式：<aphone/apad/iphone/ipad>_<操作系统>_<设备型号>
		// 设备型号有可能有_所以设定分隔个数是3
		String[] devArr = originDev.split("_", 3);
		String origPlatTypeStr = devArr[0].trim().toLowerCase();
		if (origPlatTypeStr.startsWith("iphone")) {
			return "iphone";
		} else if (origPlatTypeStr.startsWith("aphone")) {
			return "aphone";
		} else if (origPlatTypeStr.startsWith("ipad")) {
			return "ipad";
		} else if (origPlatTypeStr.startsWith("apad")) {
			return "apad";
		} else if (origPlatTypeStr.startsWith("winphone")) {
			return "wphone";
		} else if (origPlatTypeStr.startsWith("xiaomi")) {
			return "SDK合作";
		} else if (origPlatTypeStr.startsWith("winpad")) {
			return "wpad";
		} else if (origPlatTypeStr.startsWith("flash")) {
			return "flash";
		} else if (origPlatTypeStr.startsWith("tv")) {
			return "tv";
		} else {
			return "other";
		}
	}

	public static String[] strToAry(String line, String separator) {
		if (line == null || line.length() == 0)
			return null;
		return line.split(separator, -1);
	}

	public static Map<String, String> parseUrlParamToMap(String url) {
		if (StringUtils.isEmpty(url)) {
			return null;
		}
		Map<String, String> result = new HashMap<String, String>();
		String[] paramStr = url.split("\\?");
		if (paramStr.length > 1) {
			String[] param = paramStr[1].split("\\&");
			for (String key : param) {
				if(key.split("=").length == 2) {
					result.put(key.split("=")[0], key.split("=")[1]);
				} else {
					result.put(key.split("=")[0], null);
				}
			}
		}
		return result;
	}

	/**
	 * @param time
	 * @param parseFormatStr
	 * @param formatStr
	 * @return 根据指定格式，将日志中对应的时间字符串 转换成 另一个指定格式的时间字符串
	 * @throws ParseException
	 */
	public static String parseLogTimeToStr(String time, String parseFormatStr, String formatStr) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat(parseFormatStr, Locale.ENGLISH);
		return new SimpleDateFormat(formatStr).format(dateFormat.parse(time));
	}

	/**
	 * @param time
	 * @param formatStr
	 * @return 根据指定格式，将对应的字符串时间 转换为long类型
	 * @throws ParseException
	 */
	public static long transTimeToLong(String time, String formatStr) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat(formatStr);
		return dateFormat.parse(time).getTime();
	}

	/**
	 * @param time
	 * @param formatStr
	 * @return 将long类型的时间 转换成 指定格式的时间字符串
	 * @throws ParseException
	 */
	public static String transLongTimeToStr(long time, String formatStr) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat(formatStr);
		return dateFormat.format(new Date(time));
	}

	public static boolean isNumber(String str) {
		Pattern numPattern = Pattern.compile("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?|(([0-9]+))?[.])$");
		if (str.trim().equals("-") || str.trim().equals("+")) {
			return false;
		}
		Matcher m = numPattern.matcher(str);
		return m.matches();
	}
}
