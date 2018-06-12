package com.montnets.input_jdbc_es.util;



import java.sql.Connection;

import org.apache.http.HttpHost;

import com.alibaba.fastjson.JSONArray;
import com.montnets.input_jdbc_es.es.client.RestClientFactory;

/**
 * 
 * @author lvs
 * @date 2018-05-24
 * @description 程序所有常量
 */
public class Constant {

	
	/**
	 * 全局配置文件名
	 */
	public static final String GLOBAL_CONFIG_NAME = "druid.properties";

	/**集群地址,多个用|隔开*/
	public static String HOSTNAME[] = null;
	/**通信协议*/
	public static String SCHEME = "http";
	/**集群名称*/
	public static String CLUSTER_NAME = "bigData-cluster";
	/**索引INDEX名*/
	public static String INDEX[] = null;
	/**RestClientFactory连接(高级封装Rest)*/
	public 	static RestClientFactory client = null;
    
	public static  HttpHost[] HTTP_HOST = null;
	/**数据模版文件名*/
	//public static File[] MAPPING_FILE=null;
	/**数据模版*/
	public static JSONArray MAPPING_ARR=null;
	/**ES是否初始化成功标志*/
	public static Boolean cacheFalg=false;
	/**ES重连次数,连接那么多次还是连接不上就程序停止*/
	public static Integer reconnectNum=5;
	/************设置每次可最大取多少数据，超过此数据条数报错,默认60W,官方默认1W**********/
	public static Integer MAX_RESULT_WINDOW=600000;

	/**每次从数据库取最大的数据数量*/
	public static Integer MAX_DATA_COUNT=20000;
	/**数据库连接*/
	public static  Connection CONNECTION = null;
	
}
