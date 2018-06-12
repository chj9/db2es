package com.montnets.input_jdbc_es.es.client;



import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.client.RestHighLevelClient;
import org.apache.http.client.config.RequestConfig.Builder;

/**
 * 
* Copyright: Copyright (c) 2018 Montnets
* 
* @ClassName: RestClientFactory.java
* @Description: 该类的功能描述
* Es中的rest客户端
* 文档地址:
* https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.2/java-rest-high-getting-started-initialization.html 
* @version: v1.0.0
* @author: chenhj
* @date: 2018年6月6日 下午3:00:34 
*
* Modification History:
* Date         Author          Version            Description
*---------------------------------------------------------*
* 2018年6月6日     chenhj          v1.0.0               修改原因
 */
public class RestClientFactory {
	private static final Logger logger = LoggerFactory.getLogger(RestClientFactory.class);
	 public static int CONNECT_TIMEOUT_MILLIS = 10000;//连接时间
	    public static int SOCKET_TIMEOUT_MILLIS = 30000;//等待时间
	    public static int CONNECTION_REQUEST_TIMEOUT_MILLIS = 500;
	    public static int QUERY_TIMEOUT=5 * 60 * 1000; //查询超时时间设为5分钟
	    public static int MAX_CONN_PER_ROUTE = 20;
	    public static int MAX_CONN_TOTAL = 60;
	    
	    public static int BUFFER_LIMIT_BYTES=200 * 1024 * 1024; /*100mb*/
	    
	    private static HttpHost[] HTTP_HOST;
	    private RestClientBuilder builder;
	    private RestHighLevelClient restHighLevelClient;

	    private static RestClientFactory esClientSpringFactory = new RestClientFactory();

	    private RestClientFactory(){}

	    public static RestClientFactory build(Integer maxConnectNum, Integer maxConnectPerRoute,HttpHost... httpHost){
	        HTTP_HOST = httpHost;
	        MAX_CONN_TOTAL = maxConnectNum;
	        MAX_CONN_PER_ROUTE = maxConnectPerRoute;
	        return  esClientSpringFactory;
	    }

	    public static RestClientFactory build(Integer connectTimeOut, Integer socketTimeOut,
	            Integer connectionRequestTime,Integer maxConnectNum, Integer maxConnectPerRoute,HttpHost... httpHost){
	        HTTP_HOST = httpHost;
	        CONNECT_TIMEOUT_MILLIS = connectTimeOut;
	        SOCKET_TIMEOUT_MILLIS = socketTimeOut;
	        CONNECTION_REQUEST_TIMEOUT_MILLIS = connectionRequestTime;
	        MAX_CONN_TOTAL = maxConnectNum;
	        MAX_CONN_PER_ROUTE = maxConnectPerRoute;
	        return  esClientSpringFactory;
	    }


	    public void init(){
	    	
	        builder = RestClient.builder(HTTP_HOST);
	        //设置超时
	        builder.setMaxRetryTimeoutMillis(QUERY_TIMEOUT);
	        
	        
	    	//监听节点
	    	builder.setFailureListener(new RestClient.FailureListener() {
	    	    @Override
	    	    public void onFailure(HttpHost host) {
	    	    	 logger.error(host.toHostString()+">>节点出异常..请检查...");
	    	    }
	    	  
	    	});
	    	
	        setConnectTimeOutConfig();
	        setMutiConnectConfig();
	        restHighLevelClient = new RestHighLevelClient(builder);
	      //  restClient = builder.build();
	        logger.info("Initialization elasticsearch success!!");
	    }
	    
	    // 配置连接时间延时
	    public void setConnectTimeOutConfig(){
	        builder.setRequestConfigCallback(new RequestConfigCallback() {

	            @Override
	            public Builder customizeRequestConfig(Builder requestConfigBuilder) {
	                requestConfigBuilder.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
	                requestConfigBuilder.setSocketTimeout(SOCKET_TIMEOUT_MILLIS);
	                requestConfigBuilder.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MILLIS);
	    
	                return requestConfigBuilder;
	            }
	        });
	    }
	    /**
	     * 使用异步httpclient时设置并发连接数
	     */
	    public void setMutiConnectConfig(){
	        builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
	            @Override
	            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
	                httpClientBuilder.setMaxConnTotal(MAX_CONN_TOTAL);
	                httpClientBuilder.setMaxConnPerRoute(MAX_CONN_PER_ROUTE);
	                httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom()
	                        .setIoThreadCount(50)//线程数配置
	                        .setConnectTimeout(10000)
	                        .setSoTimeout(10000)
	                        .build());
	                return httpClientBuilder;
	            }
	        });
	    }
	    public  RestHighLevelClient getRhlClient() {
	        return restHighLevelClient;
	    }
	   
	    public void closeRestHighLevelClient() {
	        if (restHighLevelClient != null) {
	            try {
	            	restHighLevelClient.close();
	            } catch (IOException e) {
	            	logger.error(" Failure to shut down：",e);
	            }
	        }
	        logger.info("Close restHighLevelClient Success");
	    }
}
