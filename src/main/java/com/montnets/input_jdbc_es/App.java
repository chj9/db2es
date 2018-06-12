package com.montnets.input_jdbc_es;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.montnets.input_jdbc_es.task.MultiTableDataTask;
import com.montnets.input_jdbc_es.task.SingleTableDataTask;
import com.montnets.input_jdbc_es.util.Constant;
import com.montnets.input_jdbc_es.util.GetProperties;
/**
 * Hello world!
 */
public class App 
  {
	private static final Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args )
    {
    	try{
    		init();
    		logger.info("程序成功启动");
    	}catch(Exception e){
			logger.error("启动失败,程序退出!",e);
			System.exit(1);
    	}
    }
    private static void init() throws Exception{
    	Map<String, String>   conf=new GetProperties(Constant.GLOBAL_CONFIG_NAME).getAppSettings();
    	//是否已经启用多表机制,如果已经启用,则设为true,如果还是以前的那套,那就设为false
    	boolean multi_table = Boolean.valueOf(conf.get("multi_table").trim());
	    ExecutorService exec = Executors.newFixedThreadPool(1);      	
		// 启动插入线程
		for (int i = 0; i < 1; i++) {
			  if(multi_table){
				    MultiTableDataTask dbDataTask =  new MultiTableDataTask(conf);
					Thread instThrad = new Thread(dbDataTask);
					instThrad.setName("DbDataTask-" + i);
					exec.execute(dbDataTask);
			  }else{
				    SingleTableDataTask dbDataTask =  new SingleTableDataTask(conf);
					Thread instThrad = new Thread(dbDataTask);
					instThrad.setName("DbDataTask-" + i);
					exec.execute(dbDataTask);
			  }
		}
    }

}
