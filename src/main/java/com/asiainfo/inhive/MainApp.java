package com.asiainfo.inhive;

import java.io.IOException;
import java.util.Map;

import com.asiainfo.bo.BaseBO;

import org.springframework.context.support.FileSystemXmlApplicationContext;

public class MainApp {
	
	public static void main(String[] args) {
		// ��ȡ�����ļ���Ĭ��Ϊ��ǰĿ¼�£�Ҳ����ʹ�����в�������
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// ��ʼ��
		FileSystemXmlApplicationContext appContext = new FileSystemXmlApplicationContext(appContextPath);
		Constant con = (Constant) appContext.getBean("property");
		try {
			con.initLog();
			con.initHive();
		} catch (IOException e) {
			System.out.println("����:��ʼ���̶�����ʧ��");
			e.printStackTrace();
			appContext.close();
			return ;
		} 
		
		// ����ÿ��ҵ��
		BaseBO.setAppContext(appContext);
		BaseBO.setHadoopConfig(Constant.getHadoopConfig());
		BaseBO.setHiveConnection(Constant.getHiveConnection());
		Map<String, BaseBO> beans = BaseBO.getAppContext().getBeansOfType(BaseBO.class);
        for(BaseBO bean : beans.values()) {
        	bean.start();
        }
	}
}