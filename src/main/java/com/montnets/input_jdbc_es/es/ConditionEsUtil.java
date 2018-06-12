package com.montnets.input_jdbc_es.es;



import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class ConditionEsUtil{
	
	private QueryBuilder queryBuilder;
	
	Map<String,Object> map;
	//private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";	
	protected QueryBuilder conditionUtil(Map<String,Object> map){
		if(map==null){
			return null;
		}
		this.map=map;
		condetionValidPhone();
		return queryBuilder;
	}
	//如果需要排序把filter换回must
	private void condetionValidPhone(){
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		 for (Entry<String, Object> s : map.entrySet()) {
			 String key = s.getKey();
			 Object value = s.getValue();
			 if(value!=null){
				 String valueToS = value.toString().trim();
				 //这一步必须要求对接方按接口来,对区间进行查询
				 if(valueToS.startsWith("{")&&valueToS.endsWith("}")&&(valueToS.contains("gt")||valueToS.contains("lt"))){
					 JSONObject json = null;
					 try {
						 json = JSON.parseObject(valueToS);
					} catch (RuntimeException e) {
						throw new IllegalArgumentException("请检查参数"+key+"是否需要区间查询,区间查询格式是否填写正确,参数内容:"+value, e);
					}
					 if(json.containsKey("gt"))boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).gt(json.get("gt")));
					 if(json.containsKey("gte"))boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).gte(json.get("gte")));
					 if(json.containsKey("lt"))boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).lt(json.get("lt")));
					 if(json.containsKey("lte"))boolQueryBuilder.filter(QueryBuilders.rangeQuery(key).lte(json.get("lte")));
					 
				 }
				 //这一步是查询
				 else if("existsMustNotQuery".equals(key)){
					 String fields[]=valueToS.split(",");
					 for(String field:fields){
						 boolQueryBuilder.mustNot(QueryBuilders.existsQuery(field));
					 }
				 }else if("existsMustQuery".equals(key)){
					 String fields[]=valueToS.split(",");
					 for(String field:fields){
						 boolQueryBuilder.must(QueryBuilders.existsQuery(field));
					 }
				 }
				 else{
					 //term是代表完全匹配，即不进行分词器分析，文档中必须包含整个搜索的词汇
					 boolQueryBuilder.filter(QueryBuilders.termQuery(key,s.getValue()));
				 	}
			 }else{
				 //term是代表完全匹配，即不进行分词器分析，文档中必须包含整个搜索的词汇
				 boolQueryBuilder.mustNot(QueryBuilders.existsQuery(key));
			 }
			
	     }			

		this.queryBuilder=boolQueryBuilder;
	}
	public static void main(String[] args) throws IOException {
		 java.util.Map<String, Object> map = new HashMap<>();
	        map.put("a","{\"gt\":1000,\"lte\":2000}");
	        map.put("b", "2");
	        map.put("c", "3");
	        map.put("d", "4");
	        map.put("e", "5");
	        QueryBuilder aa = new ConditionEsUtil().conditionUtil(map);
		//System.out.println(map.get("a").toString());
			 System.out.println(aa);
		
	}
}
