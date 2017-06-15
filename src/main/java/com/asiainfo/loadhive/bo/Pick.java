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

		// У�飬�ļ���������cdr_ST_02_201607_20160710_20160721074433_21003.gz
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
		
		// �ϴ�
		try {
			String outputTmpFile = outputTmpDir + "/" + fileName;
			fileSystem.copyFromLocalFile(new Path(inputDir + "/" + fileName), new Path(outputTmpFile));
			logger.info("* Upload to hdfs success");
		} catch (IOException e) {
			logger.info("* Upload to hdfs error:" + fileName, e);
			return ;
		}

		// ���� & ���
		tableName = CreateTable(createSql, fileName);
		InsertFile(tableName, fileName);
		logger.info("* Insert table success");

		// ���ݣ�Ŀ¼��һ����д
		if (inputBakDir != null && !inputBakDir.equals("")) {
			String inputBakFile = inputBakDir + "/" + fileName;
			inputFile.renameTo(new File(inputBakFile));
		}
		inputFile.delete();
	}

	public String CheckFileName(String fileName) throws IllegalStateException {
		// У��ǰ׺
		strExplain = (BaseEL<?>) appContext.getBean("prefixMatchEL");
		strExplain.putParamMapValue("$FILE", fileName);
		strExplain.putParamMapValue("$PREFIX", inputFileNamePrefix);
		if (!(Boolean) strExplain.getResult()) {
			logger.info("������ǰ׺Ҫ�󣬲�����prefix:" + inputFileNamePrefix);
			throw new IllegalStateException();
		}
		
		/// ����
		String[] splitFileName = fileName.split("_");
		String day = splitFileName[5].substring(0, 8);
		if (! checkDup.checkfile(fileName, day)) {
			logger.info("�ļ��Ѵ������,�����ظ����� ");
			throw new IllegalStateException();
		}

		// У��ƥ���sql
		String createSql = createSqlMap.get("cdr_"+splitFileName[2]);
		if (createSql == null) {
			logger.info("û��ƥ���ҵ��sql");
			throw new IllegalStateException();
		}
		
		return createSql;
	}
	
	public String CreateTable(String createSql, String fileName) throws IllegalStateException {
		// ƴ������Ҫע����쳣
		String[] splitFileName = fileName.split("_");
		strExplain = (BaseEL<?>) appContext.getBean("substringEL");
		strExplain.putParamMapValue("$FILE", fileName);
		String tableName = tablePrefix + splitFileName[2] + "_" + strExplain.getResult();
		logger.info("going to put " + tableName);

		// �滻�ɿ�ִ�е�sql
		strExplain = (BaseEL<?>) appContext.getBean("replaceEL");
		strExplain.putParamMapValue("$FILE", createSql);
		strExplain.putParamMapValue("$OLDSTR", "$TABLENAME");
		strExplain.putParamMapValue("$NEWSTR", tableName);
		createSql = (String) strExplain.getResult();

		strExplain.putParamMapValue("$FILE", createSql);
		strExplain.putParamMapValue("$OLDSTR", "$OUTPUTDIR");
		strExplain.putParamMapValue("$NEWSTR", outputDir);
		createSql = (String) strExplain.getResult();
		
		//����
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
		
		// ��ȡ������
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
			// ���в���
			SimpleDateFormat sdf8 = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat sdf14 = new SimpleDateFormat("yyyyMMddHHmmss");

			Date date = new Date();
			String nowDay = sdf8.format(date);
			String nowDate = sdf14.format(date);
			String nowDayCut = nowDay + dailyCutTime; // ����ʱ�䣬Ĭ���賿2��

			long minusDay = (sdf8.parse(nowDay).getTime() - sdf8.parse(chargeDay).getTime()) / 86400000;
			long minusSecend = sdf14.parse(nowDate).getTime() - sdf14.parse(nowDayCut).getTime(); 
			
			/* 
			 * ��ǰ���͵��յĵ����ԼƷ�����Ϊ�����ա���������ʱ����02�㣬�ҵ�ǰ����12�ţ���ô�����������
			 * 1.�Ʒ�����11�ţ��ҵ�ǰʱ����12��01�㣬������Ϊ11��
			 * 2.�Ʒ�����11�ţ��ҵ�ǰʱ����12��03�㣬������Ϊ12��
			 * 3.�Ʒ�����12�ţ�������Ϊ12��
			 * 4.�Ʒ�����13�ż����ϵĳ�ǰ����������Ϊ12��
			 */
			if ((minusDay <= 0) || (minusDay == 1L && minusSecend < 0)) { 
				dealDate = chargeDay;
			} else {
				dealDate = nowDay;
			}
		} else { 
			// �������߼�����Ҫ����ָ���Ĺ̶�������
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
