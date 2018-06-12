package com.montnets.input_jdbc_es.task;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.montnets.input_jdbc_es.bean.EsBean;
import com.montnets.input_jdbc_es.db.ConnectionManager;
import com.montnets.input_jdbc_es.db.DBDao;
import com.montnets.input_jdbc_es.db.impl.DBDaoImpl;
import com.montnets.input_jdbc_es.es.client.ESConfigInit;
import com.montnets.input_jdbc_es.restore.exception.EsInitMonException;
import com.montnets.input_jdbc_es.service.IEsService;
import com.montnets.input_jdbc_es.service.impl.EsServiceImpl;
import com.montnets.input_jdbc_es.util.Constant;
import com.montnets.input_jdbc_es.util.LastRunUtil;
import com.montnets.input_jdbc_es.util.MyTools;
/**
* Copyright: Copyright (c) 2018 Montnets
* 
* @ClassName: DbDataTask.java
* @Description: 该类的功能描述
*多表入库程序,该程序的分表后的操作
* @version: v1.0.0
* @author: chenhj
* @date: 2018年4月9日 上午9:50:00 
*
* Modification History:
* Date         Author          Version            Description
*---------------------------------------------------------*
* 2018年6月8日     chenhj          v1.0.0               首次
 */
public class MultiTableDataTask implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(MultiTableDataTask.class);
	
	private IEsService esService;
	//是否是首次进入该程序
	private static boolean isFirst=true;
	//DB的状态
	private static boolean dbFlag = false;
	private DBDao dbDao;
	private EsBean esBean;
	private Map<String, String>  conf;
	public MultiTableDataTask(Map<String, String>  conf) throws Exception{
		this.conf=conf;
		initDB();
		logger.info("初始化数据库成功...！");
		initEs();
		logger.info("初始化ES成功...！"+Constant.cacheFalg);
		esBean=new EsBean("pb_basic_phone2", "phone");
	}
@Override
public void run() {
		esService = new EsServiceImpl();
		/****国际手机号表*******/
		 Map<String, Object> gj = new HashMap<String, Object>();
		 gj.put("name", "PB_PHONEBASIC_G");
		/*********获取分表数量 从1100开始*************/
		 String tableNumSql = "SELECT name from sysobjects where xtype='U' and name like 'PB_BASICPHONE_%'";
		 logger.info("execute sql:"+tableNumSql);
		 List<Map<String, Object>> table_list = null;
		try {
			table_list = dbDao.executeQuery(tableNumSql);
		} catch (SQLException e2) {
			logger.error("数据库执行错误"+e2);
		}
		 if(null==table_list||table_list.isEmpty()){
			  logger.error("当前数据库不存在[PB_BASICPHONE]开头的表,请检查配置是否正确...程序退出!!!");
			  Constant.client.closeRestHighLevelClient();
			  //执行完毕关闭程序
			  System.exit(1);
		 }
		 logger.info("PB_BASICPHONE分表数量:"+table_list.size());
		 //加入国际号码
		 table_list.add(gj);

	//多表插入
	for(Map<String, Object> map:table_list){
		try {
	    String current_table=(String) map.get("name");
		LastRunUtil.setRunLastPageValue("current_table", current_table);
	    selectData:while(true){
			   if(dbFlag){
					String pagestr =  LastRunUtil.getRunLastPageValue("last_id");
					Integer page = Integer.valueOf(pagestr.trim());
					String initSql  = "SELECT TOP %d * FROM  %s WHERE ID > %d*(%d-1) ORDER BY ID ASC";
					String sql = "";  
					if(page==1){
						initSql= "SELECT TOP %d * FROM  %s WHERE ID > %d ORDER BY ID ASC";
						sql =String.format(initSql,Constant.MAX_DATA_COUNT,current_table,-1);	
					}else{
						sql =String.format(initSql,Constant.MAX_DATA_COUNT,current_table,Constant.MAX_DATA_COUNT,page);	
					}
					logger.info("execute sql:"+sql);
					List<Map<String, Object>> list =dbDao.executeQuery(sql);
					if(null==list||list.isEmpty()){
							logger.info(current_table+"数据导入完毕.");
							//页数重新设为1
							LastRunUtil.setRunLastPageValue("last_id", 1+"");
							//退出当前表循环
							break selectData; 
						
					}else{
						boolean falg =esService.saveDoc(list, esBean);
						if(falg){
							LastRunUtil.setRunLastPageValue("last_id", (page+1)+"");
						}
					}
			   }else{
					initDB();
					Thread.sleep(30000);
			   }
			}
		}catch (EsInitMonException e) {
			Constant.cacheFalg=false;
		}catch (SQLException e) {
			dbFlag = false;
			logger.error("DB错误!",e);
		}catch (IllegalStateException e) {
			logger.error("非法参数",e);
			try {
				TimeUnit.SECONDS.sleep(30);
			} catch (InterruptedException e1) {
				logger.error("暂停失败!",e1);
			}
		}
    	catch (Exception e) {
			logger.error("处理异常:", e);
			try {
				dbFlag = false;
				Constant.cacheFalg=false;
				TimeUnit.SECONDS.sleep(30);
				//initDB();
			} catch (Exception e1) {
				logger.error("暂停失败!",e1);
		  } 
	   }
	}
	//以上都安全执行
	logger.info("全部数据导入完毕.程序退出!!!");
	Constant.client.closeRestHighLevelClient();
	//执行完毕关闭程序
	System.exit(1);
	//ES的异常  
	
 }
/**
 * 数据库检查
 * @throws Exception 
 */ 
private void initDB() throws Exception{
	boolean falg = true;
	//注入服务
	dbDao  =  new DBDaoImpl();
	//单次取最大数量
	String maxDataCount= conf.get("maxDataCount");
	if (MyTools.isNotEmpty(maxDataCount)) {
		Constant.MAX_DATA_COUNT = Integer.valueOf(maxDataCount);
	}else {
		logger.warn("从数据库单次取数据数量未配置或配置有误，已启用默认值："+Constant.MAX_DATA_COUNT);
	}
	int reconnect =5;//重连次数
	//初始化数据库connection连接
	try {
		//先关闭
		if(Constant.CONNECTION!=null){
			Constant.CONNECTION.close();
		}
		Constant.CONNECTION = ConnectionManager.getInstance().getConnection();
		//进行重连
		if(Constant.CONNECTION==null){
			for(int i=0;i<reconnect;i++){
				if(Constant.CONNECTION!=null)break;
				 Constant.CONNECTION = ConnectionManager.getInstance().getConnection();
			}
			//重连5次还是失败直接中止程序
			if(Constant.CONNECTION==null){
    			logger.error(reconnect+"次重连DB失败!");
    			falg=false;
    			if(isFirst)throw new Exception(reconnect+"次重连DB失败!");
			}
		}
	} catch (SQLException e) {
		logger.error("数据库连接失败!");
		falg=false;
		if(isFirst)throw new Exception("数据库连接失败!");
	}
	//后续将不再往外抛异常
	isFirst=false;
	dbFlag = falg;
	logger.info("DB初始化完成,当前DB状态:"+dbFlag);
}
/**
 * 初始化ES
 * @return
 * @throws Exception 
 */
private static void initEs() throws Exception{
	try {
		ESConfigInit es = new ESConfigInit();
	    boolean falg =es.initEsConfig();
	    Constant.cacheFalg=falg;
	  if(!falg){
		  logger.error("首次初始化ES失败！");
		  es.allClose();
		  throw new EsInitMonException("ES初始化失败...");
		  //System.exit(-1);
	  }
	} catch (Exception e) {
		logger.error("首次初始化ES失败！" + e);
		logger.info("程序退出！");
		throw e;
	}
}
}
