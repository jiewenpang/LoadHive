package com.asiainfo.loadhive.bo;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.loadhive.elexplain.BaseEL;

public class Pick extends BaseBO {
	private static final Logger logger = LoggerFactory.getLogger(Pick.class);
	private String dailyCutTime;
	private String fixedDealDay;
	private String loadSql;
	
	public void run() {
		initProperty();
		while (true) {
			String[] inputDirList = inputDirs.split(";");
			for (String inputDir : inputDirList) {
				logger.info("dealing path: " + inputDir);
				
				String[] fileNameList = new File(inputDir).list();
				for (String fileName : fileNameList) {
					ProcessFile(inputDir, fileName);
				}
			}
			return;
		}
	}
	
	public void ProcessFile(String inputDir, String fileName) {
		String tableName = null;
		String createSql = null;
		File inputFile = new File(inputDir + "/" + fileName);

		// 校验，文件名样例：cdr_ST_02_201607_20160710_20160721074433_21003.gz
		logger.info("##### Handling file:" + fileName);
		try {
			createSql = CheckFileName(fileName);
			logger.info("* Checkdup success");
		} catch (IllegalStateException e) {
			String inputDsuFile = inputDsuDir + "/" + fileName;
			logger.info("* Checkdup fail, Dsu file :" + inputFile + " to " + inputDsuFile);
			inputFile.renameTo(new File(inputDsuFile));
			return;
		}
		
		// 上传
		try {
			String outputTmpFile = outputTmpDir + "/" + fileName;
			fileSystem.copyFromLocalFile(new Path(inputDir + "/" + fileName), new Path(outputTmpFile));
			logger.info("* Upload to hdfs success");
		} catch (IOException e) {
			logger.info("* Upload to hdfs error:" + fileName, e);
			return ;
		}

		// 建表 & 插表
		tableName = CreateTable(createSql, fileName);
		InsertFile(tableName, fileName);
		logger.info("* Insert table success");

		// 备份，目录不一定可写
		if (inputBakDir != null && !inputBakDir.equals("")) {
			String inputBakFile = inputBakDir + "/" + fileName;
			inputFile.renameTo(new File(inputBakFile));
		}
		inputFile.delete();
	}

	public String CheckFileName(String fileName) throws IllegalStateException {
		// 校验前缀
		strExplain = (BaseEL<?>) appContext.getBean("prefixMatchEL");
		strExplain.putParamMapValue("$FILE", fileName);
		strExplain.putParamMapValue("$PREFIX", inputFileNamePrefix);
		if (!(Boolean) strExplain.getResult()) {
			logger.info("不符合前缀要求，不处理，prefix:" + inputFileNamePrefix);
			throw new IllegalStateException();
		}
		
		/// 查重
		String[] splitFileName = fileName.split("_");
		String day = splitFileName[5].substring(0, 8);
		if (! checkDup.checkfile(fileName, day)) {
			logger.info("文件已处理过了,无需重复处理 ");
			throw new IllegalStateException();
		}

		// 校验匹配的sql
		String createSql = createSqlMap.get("cdr_"+splitFileName[2]);
		if (createSql == null) {
			logger.info("没有匹配的业务sql");
			throw new IllegalStateException();
		}
		
		return createSql;
	}
	
	public String CreateTable(String createSql, String fileName) throws IllegalStateException {
		// 拼表名，要注意空异常
		String[] splitFileName = fileName.split("_");
		strExplain = (BaseEL<?>) appContext.getBean("substringEL");
		strExplain.putParamMapValue("$FILE", fileName);
		String tableName = tablePrefix + splitFileName[2] + "_" + strExplain.getResult();
		logger.info("going to put " + tableName);

		// 替换成可执行的sql
		strExplain = (BaseEL<?>) appContext.getBean("replaceEL");
		strExplain.putParamMapValue("$FILE", createSql);
		strExplain.putParamMapValue("$OLDSTR", "$TABLENAME");
		strExplain.putParamMapValue("$NEWSTR", tableName);
		createSql = (String) strExplain.getResult();

		strExplain.putParamMapValue("$FILE", createSql);
		strExplain.putParamMapValue("$OLDSTR", "$OUTPUTDIR");
		strExplain.putParamMapValue("$NEWSTR", outputDir);
		createSql = (String) strExplain.getResult();
		
		//建表
		try {
			executeDml(createSql);
		} catch (SQLException e) {
			logger.warn("", e);
		}
		logger.info("createSql:" + createSql);
		
		return tableName;
	}
	
	
	public void InsertFile(String tableName, String fileName) throws IllegalStateException {
		
		String[] splitFileName = fileName.split("_");
		String dealDate = null;
		
		// 获取处理日
		try { 
			dealDate = DateFormat(splitFileName[4], splitFileName[3]);
			logger.info("dealDate:" + dealDate);
		} catch (Exception e) {
			logger.info("DateFormat err!Please check!");
		}
		
		loadSql = loadSql.replace("$OUTPUTTMPDIR", outputTmpDir).replace("$FILENAME", fileName)
						.replace("$TABLENAME", tableName).replace("$CITYID", splitFileName[1])
						.replace("$DAY", splitFileName[4]).replace("$DEALDATE", dealDate);
		logger.info("loadSql:" + loadSql);
		
		try {
			Statement st = hiveConnection.createStatement();
			st.execute(loadSql);
		} catch (SQLException e) {
			logger.info("execute sql err:" + loadSql, e);
			return;
		}

		checkDup.Addfile(fileName, splitFileName[5].substring(0, 8));
	}

	public String DateFormat(String chargeDay, String chargeMonth) throws ParseException {
		String dealDate;
		
		if (dailyCutTime!=null && !dailyCutTime.equals("")) {
			// 日切操作
			SimpleDateFormat sdf8 = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat sdf14 = new SimpleDateFormat("yyyyMMddHHmmss");

			Date date = new Date();
			String nowDay = sdf8.format(date);
			String nowDate = sdf14.format(date);
			String nowDayCut = nowDay + dailyCutTime; // 日切时间，默认凌晨2点

			long minusDay = (sdf8.parse(nowDay).getTime() - sdf8.parse(chargeDay).getTime()) / 86400000;
			long minusSecend = sdf14.parse(nowDate).getTime() - sdf14.parse(nowDayCut).getTime(); 
			
			/* 
			 * 超前单和当日的单，以计费日作为处理日。例如日切时间是02点，且当前日是12号，那么有以下情况：
			 * 1.计费日是11号，且当前时间是12号01点，则处理日为11号
			 * 2.计费日是11号，且当前时间是12号03点，则处理日为12号
			 * 3.计费日是12号，则处理日为12号
			 * 4.计费日是13号及以上的超前单，则处理日为12号
			 */
			if ((minusDay <= 0) || (minusDay == 1L && minusSecend < 0)) { 
				dealDate = chargeDay;
			} else {
				dealDate = nowDay;
			}
		} else { 
			// 非日切逻辑，需要考虑指定的固定处理日
			dealDate = (fixedDealDay!=null && !fixedDealDay.equals("")) ? (chargeMonth+fixedDealDay) : chargeDay;
		}
		
		return dealDate;
	}

	public String getDailyCutTime() {
		return dailyCutTime;
	}

	public void setDailyCutTime(String dailyCutTime) {
		this.dailyCutTime = dailyCutTime;
	}

	public String getFixedDealDay() {
		return fixedDealDay;
	}

	public void setFixedDealDay(String fixedDealDay) {
		this.fixedDealDay = fixedDealDay;
	}

	public String getLoadSql() {
		return loadSql;
	}

	public void setLoadSql(String loadSql) {
		this.loadSql = loadSql;
	}
	
}
