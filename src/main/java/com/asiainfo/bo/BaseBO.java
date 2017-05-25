package com.asiainfo.bo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.elexplain.BaseEL;

public class BaseBO extends Thread {

	protected static final Logger logger = LoggerFactory.getLogger(BaseBO.class);
	protected static FileSystemXmlApplicationContext appContext;
	protected static Connection hiveConnection;
	protected BaseEL<?> strExplain;

	protected String name;
	protected String tablePrefix;
	protected String createSql;
	protected String chkDupPath;
	protected String inputPath;
	protected String inputFileNamePrefix;
	protected String inputDsuPath;
	protected String inputBak;
	protected String outputTmp;
	protected String outputPath;
	protected String outputBak;
	protected HashMap<String, String> partitionMap;

	public void run() {
		showProperty();

		try {
			executeDql("desc cdr_01_201601");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String[] files = { "mimi_dfadf", "migu_dafdga", "MG_20170404_20170405144606_08.gz" };

		strExplain = (BaseEL<?>) appContext.getBean("prefixMatchEL");
		for (String file : files) {
			logger.info("handling file:" + file);
			strExplain.putParamMapValue("$FILE", file);
			strExplain.putParamMapValue("$PREFIX", inputFileNamePrefix);
			if (!(Boolean) strExplain.getResult()) {
				continue;
			}

			strExplain = (BaseEL<?>) appContext.getBean("substringEL");
			strExplain.putParamMapValue("$FILE", file);
			String tableName = tablePrefix + strExplain.getResult();
			logger.info("going to put " + tableName);
		}
	}

	protected void showProperty() {
	}
	
	public static void executeDml(String sql) throws SQLException {
		PreparedStatement statement = null;
		
		try {
			statement = BaseBO.hiveConnection.prepareStatement(sql);
			statement.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (null != statement) {
				statement.close();
			}
		}
	}

	protected static void executeDql(String sql) throws SQLException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		ResultSetMetaData resultMetaData = null;

		try {
			// 执行HQL
			statement = BaseBO.hiveConnection.prepareStatement(sql);
			resultSet = statement.executeQuery();

			// 输出查询的列名到控制台
			resultMetaData = resultSet.getMetaData();
			int columnCount = resultMetaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				System.out.print(resultMetaData.getColumnLabel(i) + '\t');
			}
			System.out.println();

			// 输出查询结果到控制台
			while (resultSet.next()) {
				for (int i = 1; i <= columnCount; i++) {
					System.out.print(resultSet.getString(i) + '\t');
				}
				System.out.println();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (null != resultSet) {
				resultSet.close();
			}

			if (null != statement) {
				statement.close();
			}
		}
	}

	public static ApplicationContext getAppContext() {
		return appContext;
	}

	public static void setAppContext(FileSystemXmlApplicationContext appContext) {
		BaseBO.appContext = appContext;
	}

	public String getTablePrefix() {
		return tablePrefix;
	}

	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}

	public String getCreateSql() {
		return createSql;
	}

	public void setCreateSql(String createSql) {
		this.createSql = createSql;
	}

	public String getChkDupPath() {
		return chkDupPath;
	}

	public void setChkDupPath(String chkDupPath) {
		this.chkDupPath = chkDupPath;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getInputFileNamePrefix() {
		return inputFileNamePrefix;
	}

	public void setInputFileNamePrefix(String inputFileNamePrefix) {
		this.inputFileNamePrefix = inputFileNamePrefix;
	}

	public String getInputDsuPath() {
		return inputDsuPath;
	}

	public void setInputDsuPath(String inputDsuPath) {
		this.inputDsuPath = inputDsuPath;
	}

	public String getInputBak() {
		return inputBak;
	}

	public void setInputBak(String inputBak) {
		this.inputBak = inputBak;
	}

	public String getOutputTmp() {
		return outputTmp;
	}

	public void setOutputTmp(String outputTmp) {
		this.outputTmp = outputTmp;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getOutputBak() {
		return outputBak;
	}

	public void setOutputBak(String outputBak) {
		this.outputBak = outputBak;
	}

	public HashMap<String, String> getPartitionMap() {
		return partitionMap;
	}

	public void setPartitionMap(HashMap<String, String> partitionMap) {
		this.partitionMap = partitionMap;
	}

	public Connection getHiveConnection() {
		return hiveConnection;
	}

	public static void setHiveConnection(Connection hiveConnection) {
		BaseBO.hiveConnection = hiveConnection;
	}

}