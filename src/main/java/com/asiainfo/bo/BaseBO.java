package com.asiainfo.bo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.elexplain.BaseEL;
import com.asiainfo.util.CheckDup;

public abstract class BaseBO extends Thread {
	protected final static Logger logger = LoggerFactory.getLogger(BaseBO.class);

	protected static FileSystemXmlApplicationContext appContext;
	protected static Configuration hadoopConfig;
	protected static FileSystem fileSystem;
	protected static Connection hiveConnection;
	protected BaseEL<?> strExplain;
	protected CheckDup checkDup;
	
	protected String name;
	protected String tablePrefix;
	protected HashMap<String, String> createSqlMap;
	protected String chkDupDir;
	protected String inputDirs;
	protected String inputFileNamePrefix;
	protected String inputDsuDir;
	protected String inputBakDir;
	protected String outputTmpDir;
	protected String outputDir;
	protected String outputBakDir;
	protected HashMap<String, String> partitionMap;

	protected void initProperty() {
		checkDup = new CheckDup(chkDupDir, this.getName(), (chkDupDir!=null && !chkDupDir.equals("")) ? "1" : "0");
	}
	
	public abstract void run();
	
	protected static void executeDml(String sql) throws SQLException {
		PreparedStatement statement = null;
		
		try {
			statement = BaseBO.hiveConnection.prepareStatement(sql);
			statement.executeUpdate();
		} catch (SQLException e) {
			logger.warn("", e);
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
			StringBuffer tmpBuffer = new StringBuffer();
			for (int i = columnCount; i >= 1; i--) {
				tmpBuffer.append(resultMetaData.getColumnLabel(i) + '\t');
			}
			logger.info(tmpBuffer.toString());

			// 输出查询结果到控制台
			while (resultSet.next()) {
				tmpBuffer.setLength(0);
				for (int i = columnCount; i >= 1; i--) {
					tmpBuffer.append(resultSet.getString(i) + '\t');
				}
				logger.info(tmpBuffer.toString());
			}
		} catch (SQLException e) {
			logger.warn("", e);
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

	public HashMap<String, String> getCreateSqlMap() {
		return createSqlMap;
	}

	public void setCreateSqlMap(HashMap<String, String> createSqlMap) {
		this.createSqlMap = createSqlMap;
	}

	public String getChkDupDir() {
		return chkDupDir;
	}

	public void setChkDupDir(String chkDupDir) {
		this.chkDupDir = chkDupDir;
	}

	public String getInputDirs() {
		return inputDirs;
	}

	public void setInputDirs(String inputDirs) {
		this.inputDirs = inputDirs;
	}

	public String getInputFileNamePrefix() {
		return inputFileNamePrefix;
	}

	public void setInputFileNamePrefix(String inputFileNamePrefix) {
		this.inputFileNamePrefix = inputFileNamePrefix;
	}

	public String getInputDsuDir() {
		return inputDsuDir;
	}

	public void setInputDsuDir(String inputDsuDir) {
		this.inputDsuDir = inputDsuDir;
	}

	public String getInputBakDir() {
		return inputBakDir;
	}

	public void setInputBakDir(String inputBakDir) {
		this.inputBakDir = inputBakDir;
	}

	public String getOutputTmpDir() {
		return outputTmpDir;
	}

	public void setOutputTmpDir(String outputTmpDir) {
		this.outputTmpDir = outputTmpDir;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public String getOutputBakDir() {
		return outputBakDir;
	}

	public void setOutputBakDir(String outputBakDir) {
		this.outputBakDir = outputBakDir;
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

	public static Configuration getHadoopConfig() {
		return hadoopConfig;
	}

	public static void setHadoopConfig(Configuration hadoopConfig) {
		BaseBO.hadoopConfig = hadoopConfig;
		try {
			fileSystem = FileSystem.get(hadoopConfig);
		} catch (IOException e) {
			logger.warn("", e);
		}
	}

}