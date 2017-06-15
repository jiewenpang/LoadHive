package com.asiainfo.loadhive;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

public class Constant {
	private static Configuration hadoopConfig = new Configuration();
	public static HashMap<String, String> propertyMap;
	private static Connection hiveConnection;

	public void initLog() throws IOException {
		System.setProperty("LOG_HOME", (String) propertyMap.get("LOG_PATH"));

		File logbackConfFile = new File((String) propertyMap.get("LOGBACK_CONF_FILE"));
		if (!logbackConfFile.canRead()) {
			System.out.println("日志配置文件logback.xml不存在:" + logbackConfFile.getAbsolutePath());
			throw new IllegalStateException();
		}

		if (System.getProperty("logback.configurationFile") == null) {
			System.setProperty("logback.configurationFile", logbackConfFile.getAbsolutePath());
		}
	}

	public void initHive() throws IOException {
		initHadoop();
		
		try {
			Class.forName(propertyMap.get("class"));
			hiveConnection = DriverManager.getConnection(propertyMap.get("connString"), "", "");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void initHadoop() throws IOException {
		if ("true".equals(hadoopConfig.get("hadoop.security.authorization")) 
				&& "kerberos".equals(hadoopConfig.get("hadoop.security.authentication"))) {
			
			System.setProperty("java.security.krb5.conf", propertyMap.get("krb5conf"));
			System.setProperty("java.security.auth.login.config", propertyMap.get("jaasconf"));
			hadoopConfig.set("username.client.keytab.file", propertyMap.get("keytab"));
			hadoopConfig.set("username.client.kerberos.principal", propertyMap.get("principal"));
			
			UserGroupInformation.setConfiguration(hadoopConfig);
			SecurityUtil.login(hadoopConfig, propertyMap.get("krytab"), propertyMap.get("principal"));
		} 
	}

	public static Connection getHiveConnection() {
		return hiveConnection;
	}
	
	public void setPropertyMap(HashMap<String, String> propertyMap) {
		Constant.propertyMap = propertyMap;
	}

	public static Configuration getHadoopConfig() {
		return hadoopConfig;
	}

	public static void setHadoopConfig(Configuration hadoopConfig) {
		Constant.hadoopConfig = hadoopConfig;
	}

}
