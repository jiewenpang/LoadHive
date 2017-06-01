package com.asiainfo.bo;

import java.sql.SQLException;

import com.asiainfo.elexplain.BaseEL;

public class Migu extends BaseBO {

    public void run() {
    	// just for demo
		initProperty();

		try {
			executeDql("desc cdr_02_201607");
		} catch (SQLException e) {
			logger.warn("", e);
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
    
}