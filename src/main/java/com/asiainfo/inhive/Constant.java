package com.asiainfo.inhive;

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
	private static Configuration m_config = new Configuration();
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
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void initHadoop() throws IOException {
		if ("true".equals(m_config.get("hadoop.security.authorization")) 
				&& "kerberos".equals(m_config.get("hadoop.security.authentication"))) {
			
			System.setProperty("java.security.krb5.conf", propertyMap.get("krb5conf"));
			System.setProperty("java.security.auth.login.config", propertyMap.get("jaasconf"));
			m_config.set("username.client.keytab.file", propertyMap.get("keytab"));
			m_config.set("username.client.kerberos.principal", propertyMap.get("principal"));
			
			UserGroupInformation.setConfiguration(m_config);
			SecurityUtil.login(m_config, propertyMap.get("krytab"), propertyMap.get("principal"));
		} 
	}

	public static Connection getHiveConnection() {
		return hiveConnection;
	}
	
	public void setPropertyMap(HashMap<String, String> propertyMap) {
		Constant.propertyMap = propertyMap;
	}

}
