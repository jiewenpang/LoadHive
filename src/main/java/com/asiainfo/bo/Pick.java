package com.asiainfo.bo;

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

import com.asiainfo.elexplain.BaseEL;
import com.asiainfo.util.CheckDup;
import com.asiainfo.util.StringUtil;

public class Pick extends BaseBO {
	private static final Logger logger = LoggerFactory.getLogger(Pick.class);
	private String dailyCutTime;
	private String dealDay32;
	private Boolean dealNormal;

	protected void initProperty() {
		checkDup = new CheckDup(chkDupDir, name, (chkDupDir!=null && !chkDupDir.equals("")) ? "1" : "0");
	}
	
	public void run() {
		logger.info(name);
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
			stop();
		}
	}
	
	public void ProcessFile(String inputDir, String fileName) {
		String tableName = null;
		String createSql = null;
		File inputFile = new File(inputDir + "/" + fileName);

		// 校验，文件名样例：cdr_ST_02_201607_20160710_20160721074433_21003.gz
		logger.info("handling file:" + fileName);
		try {
			createSql = CheckFileName(fileName);
		} catch (IllegalStateException e) {
			String inputDsuFile = inputDsuDir + "/" + fileName;
			logger.info("挂起文件:" + inputFile + " to " + inputDsuFile);
			inputFile.renameTo(new File(inputDsuFile));
			return;
		}
		
		// 上传
		try {
			String outputTmpFile = outputTmpDir + "/" + fileName;
			fileSystem.copyFromLocalFile(new Path(inputDir + "/" + fileName), new Path(outputTmpFile));
			logger.info("put to hdfs success");
		} catch (IOException e) {
			logger.info("put to hdfs err:" + fileName, e);
			return ;
		}

		// 建表 & 插表
		tableName = CreateTable(createSql, fileName);
		InsertFile(tableName, fileName);

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
			e.printStackTrace();
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
		
		String loadSql = "load data inpath '$OUTPUTTMPDIR/$FILENAME' into table $TABLENAME partition(cityid='$CITYID',dt='$DAY',dealdate='$DEALDATE')"
				.replace("$OUTPUTTMPDIR", outputTmpDir).replace("$FILENAME", fileName)
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

	public String DateFormat(String day, String month) throws ParseException {
		String dealDate;
		SimpleDateFormat sdf8 = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf14 = new SimpleDateFormat("yyyyMMddHHmmss");

		Date date = new Date();
		String nowDay = sdf8.format(date);
		String nowDate = sdf14.format(date);
		String nowDayCut = nowDay + dailyCutTime; // 日切时间，默认凌晨2点

		int minusDay = StringUtil.ParseInt(nowDay) - StringUtil.ParseInt(day);
		long minusSecend = sdf14.parse(nowDate).getTime() - sdf14.parse(nowDayCut).getTime();

		logger.info("minusDay:" + minusDay);
		logger.info("minusSecend:" + minusSecend);

		if (!dealNormal) {
			if ((minusDay <= 0) || (minusDay == 1 && minusSecend < 0)) {
				dealDate = day;
			} else {
				dealDate = nowDay;
			}
		} else {
			dealDate = (dealDay32!=null && dealDay32.equals("")) ? (month+dealDay32) : day;
		}
		return dealDate;
	}

	public String getDailyCutTime() {
		return dailyCutTime;
	}

	public void setDailyCutTime(String dailyCutTime) {
		this.dailyCutTime = dailyCutTime;
	}

	public String getDealDay32() {
		return dealDay32;
	}

	public void setDealDay32(String dealDay32) {
		this.dealDay32 = dealDay32;
	}

	public Boolean getDealNormal() {
		return dealNormal;
	}

	public void setDealNormal(Boolean dealNormal) {
		this.dealNormal = dealNormal;
	}
	
}
