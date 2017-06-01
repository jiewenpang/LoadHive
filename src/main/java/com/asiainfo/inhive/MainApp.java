package com.asiainfo.inhive;

import java.io.IOException;
import java.util.Map;

import com.asiainfo.bo.BaseBO;

import org.springframework.context.support.FileSystemXmlApplicationContext;

public class MainApp {
	
	public static void main(String[] args) {
		// 读取配置文件，默认为当前目录下，也可以使用运行参数配置
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// 初始化
		FileSystemXmlApplicationContext appContext = new FileSystemXmlApplicationContext(appContextPath);
		Constant con = (Constant) appContext.getBean("property");
		try {
			con.initLog();
			con.initHive();
		} catch (IOException e) {
			System.out.println("错误:初始化固定参数失败");
			e.printStackTrace();
			appContext.close();
			return ;
		} 
		
		// 启动每个业务
		BaseBO.setAppContext(appContext);
		BaseBO.setHadoopConfig(Constant.getHadoopConfig());
		BaseBO.setHiveConnection(Constant.getHiveConnection());
		Map<String, BaseBO> beans = BaseBO.getAppContext().getBeansOfType(BaseBO.class);
        for(BaseBO bean : beans.values()) {
        	bean.start();
        }
	}
}