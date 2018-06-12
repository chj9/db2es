package com.montnets.input_jdbc_es.es;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.montnets.input_jdbc_es.util.MyTools;



/**
 * 
* Copyright: Copyright (c) 2018 Montnets
* 
* @ClassName: InsertIndexAction.java
* @Description: 该类的功能描述
*ES插入数据公共类  
* @version: v1.0.0
* @author: chenhj
* @date: 2018年6月6日 下午3:17:25 
*
* Modification History:
* Date         Author          Version            Description
*---------------------------------------------------------*
* 2018年6月6日     chenhj          v1.0.0               修改原因
 */
public class InsertIndexAction {
	  /**索引库*/
	  private String index;
	  /**索引表*/
	  private String type;	  
	  private RestHighLevelClient rhlClient;
	  private static final String FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSS";
	  private static final Logger LOG = LoggerFactory.getLogger(InsertIndexAction.class);
		//private static final Logger LOGGE = LoggerFactory.getLogger("errorInfo");
	public InsertIndexAction(RestHighLevelClient rhlClient,String index,String type){
		this.index=index;
		this.type =type;
		this.rhlClient=rhlClient;
	}
	/**
	 * 批量插入ES库
	 * @param list 需要插入的数据列表
	 * @param idkey 每条数据的主键名
	 * @param docAsUpsert 是否需要存在更新,不存在插入   true:更新操作,存在更新,不存在插入    false:直接插入,如果数据存在直接覆盖
	 */
	public  boolean insertBulkIndex(List<Map<String,Object>> list,String idkey,boolean docAsUpsert) throws Exception{
		 BulkRequest request = new BulkRequest();
		 boolean falg=true;
		 try {
		 for(Map<String,Object> map:list){
			     //有ID则是修改没有则是新增			
				 String id = String.valueOf(map.get(idkey));
				 //时间补全
				 timeOfCompletion(map);
				 
				 if(MyTools.isEmpty(id)&&"null".equals(id)){
				       throw new RuntimeException("主键必须设值!!!当前主键"+idkey+"="+id);
				 } 
				 if(docAsUpsert){
					 //存在更新,不存在插入
					 request.add(new UpdateRequest(index,type,id).doc(map,XContentType.JSON).docAsUpsert(docAsUpsert));
				 }else{
					 //直接插入,存在直接覆盖
					 request.add(new IndexRequest(index,type,id).source(map,XContentType.JSON));
				 }
		 }
		 //如果不为空就写入
		 if(request.numberOfActions()>0){
			 BulkResponse bulkResponse = rhlClient.bulk(request);
			 //响应失败的数据过来写入日志
			 if(bulkResponse.hasFailures()){
				 for (BulkItemResponse bulkItemResponse : bulkResponse) {
					    if (bulkItemResponse.isFailed()) { 
					        BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
					        LOG.error("写入失败!数据:"+failure.toString());
					        falg=false;
					    }
					}
			 }
		 } 
		} catch (Exception e) {
			//e.printStackTrace();
			//String msg=e.getMessage();
			//if(MyTools.isEmpty(msg)){
				LOG.error("ID参数："+MyTools.toString(list));
			//	falg=true;
			//}else{
			//	LOG.error("连接ES失败!",e);
			//	falg=false;
				throw e;
		//	}
		}
	    return falg; 
	}
	/**
	 * 时间补全
	 */
	private static void  timeOfCompletion(Map<String,Object> map){
		
		
		
    	String createtm = map.get("createtm")+"";
    	String lastupdatetm = map.get("lastupdatetm")+"";
    	String lastdltm = map.get("lastdltm")+"";
    	String lastrpttm = map.get("lastrpttm")+"";
    	if(MyTools.isNotEmpty(createtm)&&!"null".equalsIgnoreCase(createtm)){
    		map.put("createtm", MyTools.complementTime2(createtm,FORMAT));
    	}
    	if(MyTools.isNotEmpty(lastupdatetm)&&!"null".equalsIgnoreCase(createtm)){
    		map.put("lastupdatetm", MyTools.complementTime2(lastupdatetm,FORMAT));
    	}
    	if(MyTools.isNotEmpty(lastdltm)&&!"null".equalsIgnoreCase(createtm)){
    		map.put("lastdltm", MyTools.complementTime2(lastdltm,FORMAT));
    	}
    	if(MyTools.isNotEmpty(lastrpttm)&&!"null".equalsIgnoreCase(createtm)){
    		map.put("lastrpttm", MyTools.complementTime2(lastrpttm,FORMAT));
    	}
    	
	}
	public static void main(String[] args) {
		Map<String,Object> map = new HashMap<>();
		map.put("lastupdatetm","1900-01-01 00:00:00.0000000");
		timeOfCompletion(map);
		System.out.println(map);
		
	}
}
