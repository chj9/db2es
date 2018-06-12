package com.montnets.input_jdbc_es.es.client;



import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.montnets.input_jdbc_es.util.Constant;
import com.montnets.input_jdbc_es.util.GetProperties;
import com.montnets.input_jdbc_es.util.MyTools;
import com.montnets.input_jdbc_es.util.NetAddrUtil;




/**
 * 
* Copyright: Copyright (c) 2018 Montnets
* 
* @ClassName: ESConfigInit.java
* @Description: 该类的功能描述
*主要对一些索引数据模板这些的检查
* @version: v1.0.0
* @author: chenhj
* @date: 2018年4月9日 下午2:50:43 
*
* Modification History:
* Date         Author          Version            Description
*---------------------------------------------------------*
* 2018年4月9日     chenhj          v1.0.0               修改原因
 */
public class ESConfigInit {
	
	private static final Logger logger = LoggerFactory.getLogger(ESConfigInit.class);
	Map<String, String> conf = new HashMap<String, String>();
	public ESConfigInit(){
		conf=new GetProperties(Constant.GLOBAL_CONFIG_NAME).getAppSettings();
	}
	/**
	 * 初始化一些配置
	 */
	public synchronized boolean initEsConfig() throws Exception{
		
		boolean  falg=true;
				
		if(conf==null||conf.isEmpty()){
			logger.error("es配置为空");
			falg=false;
			return falg;
		}

		// 通信协议
		 String scheme= conf.get("es.scheme");
		 //集群IP
		 String hostname[]=conf.get("es.servers").split(",");
		 //indexs
		 String indexs[] = conf.get("es.index").split(",");
		 // 集群名称
		 String clusterName= conf.get("es.clusterName");
    	/****************ES初始化*******************/
		//集群IP
		Constant.HOSTNAME = hostname;
		if (Constant.HOSTNAME==null) {
			logger.error("集群IP不能为空!");
			falg=false;
			return falg;
		}
		if (MyTools.isNotEmpty(scheme)) {
			Constant.SCHEME = scheme;
		}else {
			logger.warn("通信协议未配置或配置有误，已启用默认值："+Constant.SCHEME);
		}

		if (MyTools.isNotEmpty(clusterName)) {
			Constant.CLUSTER_NAME = clusterName;
		}else {
			logger.warn("集群名称未配置或配置有误，已启用默认值："+Constant.CLUSTER_NAME);
		}
		//数据INDEX
		Constant.INDEX = indexs;
		if (Constant.INDEX==null) {
			logger.error("索引INDEX不能为空!");
			falg=false;
			return falg;
		}

		//初始化ES集群IP
		 Constant.HTTP_HOST=new HttpHost[hostname.length];
    	 for(int i=0;i<hostname.length;i++){
    		 try {
	    		 String url = hostname[i];
	    		 NetAddrUtil addr = new  NetAddrUtil();  
	    	     addr.IpPortFromUrl(url);
	        	 HttpHost httpHost = new HttpHost(addr.getIp(),addr.getPort(),Constant.SCHEME);
	        	 Constant.HTTP_HOST[i]=httpHost;
 			} catch (Exception e) {
				throw new RuntimeException("执行异常",e);
			}
    	 }
		//初始化client
    	allClose();//管他三七二十一,先走关闭一波
		if (Constant.client==null||Constant.client.getRhlClient()==null) {
			//5次重连
			for(int i=0;i<Constant.reconnectNum;i++){
				Constant.client =RestClientFactory.build(60,20,Constant.HTTP_HOST);
				if(Constant.client!=null)Constant.client.init();
	    		//重连5次还是失败直接中止程序
	    		if(Constant.client==null||Constant.client.getRhlClient()==null){
	    			logger.debug(Constant.reconnectNum+"次重连ES失败!"+Constant.reconnectNum+"失败后程序停止");
	    			if(i==Constant.reconnectNum-1){
	    				logger.error("连接ES集群出错");
	    				falg=false;
	    				return falg;
	    			}
	    			//为避免太频繁线程睡眠会
	    			//Thread.sleep(3000);
	    		}else{
	    			break;//初始化成功,退出循环
	    		}
    		}
		}
		//判断index是否存在,不存在的话创建一个
		for(String index:Constant.INDEX){
			boolean exist = isExistsIndex(index);
			if(!exist){
				if(!createIndex(index)){
					logger.error("索引创建失败");
					allClose();//关闭连接
					falg=false;
					return falg;
			};
		  }
		}
		logger.info("ES系统配置初始化完成..."+"初始状态为:"+falg);
		return falg;
		/*******************ES初始化结束***************************/
	}
	 /**
     * 判断指定的索引名是否存在
     * @return  存在：true; 不存在：false;
     */
	public boolean isExistsIndex(String index){
        boolean isExists=true;
		try {
			RestClient restClient = Constant.client.getRhlClient().getLowLevelClient();
	        Response response = restClient.performRequest("HEAD","/"+index,Collections.<String, String>emptyMap());
	        isExists =response.getStatusLine().getReasonPhrase().equals("OK");
		} catch (IOException e) {
			logger.error("检查INDEX是否存在报错",e);
			isExists=false;
		}
        return isExists;
    }
    /**
     * 如果索引库不存在则创建一个
     * @return  成功：true; 失败：false;
     */
  public boolean createIndex(String index){
    	boolean falg = true;
		//数据模版
		 String mapping_file_path[] = conf.get("es.mappingfile").split(",");
    	//数据模板
		if (mapping_file_path==null) {
			logger.error("数据模板文件路径配置不能为空!");
			return false;
		}else{
			Constant.MAPPING_ARR=new JSONArray();
			for(int i=0;i<mapping_file_path.length;i++){
				String pathName = mapping_file_path[i];
					InputStream in = ESConfigInit.class.getClassLoader().getResourceAsStream(pathName);
					String mapping = MyTools.inputstr2Str(in,"UTF-8");
					if(MyTools.isEmpty(mapping)){
							logger.error("数据模板文件不存在！,检查路径是否输入正确,异常文件名:"+pathName);
							return false;
					}
					JSONObject json = JSON.parseObject(mapping);
					Constant.MAPPING_ARR.add(json);
			}
		}
	String indexflag = null;//标志位
	JSONObject mapping = null;
	//循环找出对应index的数据模板
	getMapping:for(Object f:Constant.MAPPING_ARR){
		mapping = (JSONObject) f;
		if(mapping.getString("index").equalsIgnoreCase(index)){
			indexflag=mapping.getString("index");
			break getMapping;//跳出内循环
		}
	}
	if(null==indexflag||null==mapping||mapping.isEmpty()){
		logger.error(index+"找不到指定的数据模板,查看文件是否正确...索引名:"+index);
		return false;
	}
	//开始创建库
    CreateIndexRequest request = new CreateIndexRequest(index); 
    	try {
			//加载数据类型
	    	request.mapping(index,mapping.getString("mappings"),XContentType.JSON);
	    	//分片数
	    	request.settings(mapping.getString("settings"),XContentType.JSON);
			CreateIndexResponse createIndexResponse = Constant.client.getRhlClient().indices().create(request);
			falg = createIndexResponse.isAcknowledged();
			if(falg){
				//设置查询单次返回最大值
				maxResultWindow(index);
			}
			logger.info("创建索引库"+index+",状态为:"+falg);
		} catch (IOException e) {
			logger.error("创建INDEX报错",e);
			falg=false;
		}catch (NullPointerException e) {
			logger.error("模板文件中的mappings或settings不能为空",e);
			falg=false;
		}
    	return falg;
    }
    /**
     * 设置每次可最大取多少数据，超过此数据条数报错
     * @throws IOException 
     */
    private void maxResultWindow(String index) throws IOException{
    	RestClient restClient = Constant.client.getRhlClient().getLowLevelClient();
        try {
        	JSONObject json = new JSONObject();
        	JSONObject json1 = new JSONObject();
        	json1.put("max_result_window", Constant.MAX_RESULT_WINDOW+"");
        	json.put("index",json1);
   		 	String source =json.toJSONString();
   		 	HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
			restClient.performRequest("PUT","/"+index+"/_settings",Collections.<String, String>emptyMap(),entity);
		} catch (IOException e) {
			throw e;
		}
    }
    /**
     * 关闭ES客户端
     */
	public void allClose() {
		synchronized(this){
			if(Constant.client!=null){
				Constant.client.closeRestHighLevelClient();
				Constant.client = null;
			}
		}
	}
}
