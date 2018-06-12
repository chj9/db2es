package com.montnets.input_jdbc_es.util;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**  
 * @Title:  LastRunUtil.java   
 * @Description:  修改页数配置文件 
 * @author: chenhongjie     
 * @date:   2018年4月11日 上午11:39:50   
 * @version V1.0 
 */
public class LastRunUtil {

		private static final Logger LOG = LoggerFactory.getLogger(LastRunUtil.class);
		private static String lastRun ="last_run_page.properties";
		private static String lastRunPath=null;
		private static Map<String,String> appSettings = new HashMap<String,String>();


		private LastRunUtil(){
			initRunPage();
		}
		private void initRunPage(){
			InputStream in = null;
			lastRunPath=lastRun;
			try{
				//获取resource中的配置
				File file =new File(lastRun);
				if(file.exists()){
					in=new FileInputStream(file);
				}else{
					in=GetProperties.class.getClassLoader().getResourceAsStream(lastRun);
				}
				//获取项目同级的配置
				//
				Properties prop = new Properties();
				prop.load(in);
				Set<Entry<Object, Object>> buf = prop.entrySet();
				Iterator<Entry<Object, Object>> it = buf.iterator();
				while(it.hasNext()){
					Entry<Object, Object> t = it.next();
					appSettings.put((String)t.getKey(), (String)t.getValue());
				}
			}catch(IOException e){
				LOG.error("加载系统参数失败!",e);
			}finally{
				if(null != in){
					try {
						in.close();
					} catch (IOException e) {
						LOG.error("加载系统参数失败!",e);
					}
				}
			}
		}
		public synchronized static void setRunLastPageValue(String key,String value) {
			if(MyTools.isEmpty(lastRunPath)){
				new LastRunUtil();
			}
				//修改前重新加载一次
			    InputStream in = null;
			    FileOutputStream fos =null;
			 try { 	
			    in=new FileInputStream(new File(lastRunPath));
				Properties prop = new Properties();
				prop.load(in);
				prop.setProperty(key, value);     
	           //文件输出流     
	        	fos=new FileOutputStream(lastRunPath);     
	            // 将Properties集合保存到流中     
	            prop.store(fos, "Copyright (c) Last Run Page");     
	        } catch (FileNotFoundException e) {    
	            e.printStackTrace();    
	        } catch (IOException e) {    
	            e.printStackTrace();    
	  
	        }finally {
	        	try {
	        	if(in!=null)in.close();
	        	if(fos!=null)fos.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}   
		}
		public synchronized static String getRunLastPageValue(String key){
			//if(MyTools.isEmpty(lastRunPath)){
				new LastRunUtil();
			//}
		   return appSettings.get(key);
		}
		/**
		 * 方法测试
		 * @param args
		 * @throws IOException 
		 */
		  public static void main(String[] args) throws IOException {
			  LastRunUtil.setRunLastPageValue("last_id","0");
			  	System.out.println(LastRunUtil.getRunLastPageValue("last_id"));
		  }
		
	}
