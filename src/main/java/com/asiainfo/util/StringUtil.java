package com.asiainfo.util;

import java.util.HashMap;

public class StringUtil {

	public static HashMap<String, String> toMAp(String attr) {

		HashMap<String, String> map = new HashMap<String, String>();
		attr = attr.replaceAll("\"", "");// 以防万一传进来的带""
		String[] attrs = attr.split("\\&", 0);

		for (int i = 0; i < attrs.length; i++) {
			if (attrs[i] != null) {
				String[] atrr = attrs[i].split("\\=");
				if (atrr.length == 2) {
					map.put(atrr[0], atrr[1]);
				} else {
					// 传过来的键值对有问题
				}
			}
		}

		return map;
	}

	// 填补NULL
	public static String addSpaces(int len, String str) {
		String strcopy = str;
		if (str.length() < len) {
			for (int i = 0; i < len - str.length(); i++) {
				strcopy = strcopy + "\0";
			}
			str = strcopy;
		}
		return str;
	}

	public static int ParseInt(String str) {
		if (str==null) {
			return 0;
		}
		
		String str_tmp = str.trim();
		try {
			return Integer.parseInt(str_tmp); // "123"
		} catch (NumberFormatException e) {
			int index = 0;
			int len = str_tmp.length();

			try { // "123abc"
				for (index = 0; index < len; index++) {
					if (str_tmp.charAt(index) < '0' || str_tmp.charAt(index) > '9') {
						if (index == 0 && (str_tmp.charAt(index) == '+' || str_tmp.charAt(index) == '-')) { // "-1234.56abc"
							continue;
						}
						break;
					}
				}
				return Integer.parseInt(str_tmp.substring(0, index));
			} catch (NumberFormatException e1) { // 值为""
				return 0;
			} 
		}
	}

	public static long ParseLong(String str) {
		if (str==null) {
			return 0L;
		}
		
		String str_tmp = str.trim();
		try {
			return Long.parseLong(str_tmp); // "123456"
		} catch (NumberFormatException e) {
			int index = 0;
			int len = str_tmp.length();

			try { // "1234.56"
				for (index = 0; index < len; index++) {
					if (str_tmp.charAt(index) < '0' || str_tmp.charAt(index) > '9') {
						if (index == 0 && (str_tmp.charAt(index) == '+' || str_tmp.charAt(index) == '-')) { // "-1234.56abc"
							continue;
						}
						break;
					}
				}

				return Long.parseLong(str_tmp.substring(0, index));
			} catch (NumberFormatException e1) { // 值为""
				return 0L;
			} 
		}
	}


	public static double ParseDouble(String str) {
		if (str==null) {
			return 0.0;
		}
		
		String str_tmp = str.trim();
		try {
			return Double.parseDouble(str_tmp); // "1234.56"
		} catch (NumberFormatException e) {

			try { // "1234.56abc"
				int index = 0;
				int len = str_tmp.length();
				
				for (index = 0; index < len; index++) {
					if ((str_tmp.charAt(index) < '0' || str_tmp.charAt(index) > '9') && str_tmp.charAt(index) != '.') {
						if (index == 0 && (str_tmp.charAt(index) == '+' || str_tmp.charAt(index) == '-')) { // "-1234.56abc"
							continue;
						}
						break;
					}
				}

				return Double.parseDouble(str_tmp.substring(0, index));
			} catch (NumberFormatException e1) { // 值为 "+" "" "abc"
				return 0.0;
			} 
		}
	}

}
