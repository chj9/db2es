package com.montnets.input_jdbc_es.task;
import java.sql.SQLException;
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
 * 
* Copyright: Copyright (c) 2018 Montnets
* 
* @ClassName: DbDataTask.java
* @Description: 该类的功能描述
*单表入ES库程序,该程序是在还没分表之前操作的
* @version: v1.0.0
* @author: chenhj
* @date: 2018年4月9日 上午9:50:00 
*
* Modification History:
* Date         Author          Version            Description
*---------------------------------------------------------*
* 2018年4月8日     chenhj          v1.0.0               首次
 */
public class SingleTableDataTask implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(SingleTableDataTask.class);
	//是否是首次进入该程序
	private static boolean isFirst=true;
	//DB的状态
	private static boolean dbFlag = false;
	private IEsService esService;
	private DBDao dbDao;
	private EsBean esBean;
	private Map<String, String>  conf;
	String current_table="PB_BASIC_PHONE";
	public SingleTableDataTask(Map<String, String>  conf) throws Exception{
		this.conf=conf;
		initDB();
		logger.info("初始化数据库成功...！");
		initEs();
		logger.info("初始化ES成功...！"+Constant.cacheFalg);
		esBean=new EsBean("pb_basic_phone", "phone");
	}
	@Override
	public void run() {	
		esService = new EsServiceImpl();
		current_table =conf.get("tableName");
	    logger.info("当前线程执行表:"+current_table);
	    while(true){
	    	try {
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
							//退出当前表循环
							break; 
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
 }
	/**
	 * 数据库检查
	 * @throws SQLException 
	 * @throws Exception 
	 */ 
	private void initDB() throws SQLException{
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
	    			//throw new Exception(reconnect+"次重连ES失败!");
				}
			}
		} catch (SQLException e) {
			logger.error("数据库连接失败!");
			falg=false;
			//throw new Exception("数据库连接失败!");
		}
		if(isFirst){
			//查询总数,获得总页数,主要做记录使用
			Long rowCount = null;
			try {
				rowCount = dbDao.tableCount(String.format("select count(*) AS counts from %s",current_table));
	    		//(totalRecord  +  pageSize  - 1) / pageSize
	    		Long pages = (rowCount+Constant.MAX_DATA_COUNT-1)/Constant.MAX_DATA_COUNT;//获得页数
	    		//该日志作为记录
	    		logger.info("本次数据总数为:"+rowCount+";每次取:"+Constant.MAX_DATA_COUNT+";取"+pages+"次");
	    		//后续的自动初始化不允许进入
	    		isFirst=false;
			} catch (SQLException e) {
				logger.error("查询数据库总数失败!,抛出异常:"+e);
				falg=false;
				//第一次启动抛出异常,让程序停止
				throw e;
			}
		}
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
