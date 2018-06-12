package com.montnets.input_jdbc_es.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.ContentTooLongException;
import org.apache.http.conn.ConnectTimeoutException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.montnets.input_jdbc_es.bean.EsBean;
import com.montnets.input_jdbc_es.es.ConditionEsUtil;
import com.montnets.input_jdbc_es.es.DelIndexAction;
import com.montnets.input_jdbc_es.es.InsertIndexAction;
import com.montnets.input_jdbc_es.es.SearchIndexAction;
import com.montnets.input_jdbc_es.es.client.ESConfigInit;
import com.montnets.input_jdbc_es.restore.exception.EsInitMonException;
import com.montnets.input_jdbc_es.service.IEsService;
import com.montnets.input_jdbc_es.util.Constant;
import com.montnets.input_jdbc_es.util.MyTools;
/**
 * @Title:  EsServiceImpl.java   
 * @Description:  ES操作接口实现类
 * @author: chenhongjie     
 * @date:   2018年5月25日 下午2:09:11   
 * @version V1.0
 */
public class EsServiceImpl extends ConditionEsUtil  implements IEsService {
	private static final Logger LOG = LoggerFactory.getLogger(EsServiceImpl.class);
	RestHighLevelClient client = null;
	public EsServiceImpl(){
		//检查连接
		try {
			client=Constant.client.getRhlClient();
		} catch (NullPointerException e) {
			Constant.cacheFalg=false;
		}
		if(null==client)Constant.cacheFalg=false;
	}
	@Override
	public boolean saveDoc(List<Map<String, Object>> list,EsBean esBean) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		if(MyTools.isEmpty(esBean.getIdkey()))throw new NullPointerException("idkey 不能为null或空");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),false))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		//存入ES中
	 	try {
	 		InsertIndexAction insert = new InsertIndexAction(client,esBean.getIndex(),esBean.getIndex());
	 		return insert.insertBulkIndex(list,esBean.getIdkey(),false);
		}catch (IOException e) {
			//该异常不处理
			if(e.getMessage().contains("listener timeout after waiting")){
				LOG.error("listener timeout after waiting:"+e);
				return false;
			}else{
				throw e;
			}
		}catch(IllegalStateException e) {
			if(e.getMessage().contains("STOPPED")){
				Constant.cacheFalg=false;
				LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
			}else{
				 throw e;
			 }
		}catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常,ES存不进,返回true:"+e);
				return true;
			}
		}
		return false;	
	}
	@Override
	public List<Map<String, Object>> searchDoc(Map<String, Object> map,EsBean esBean,String ...includeFields) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
	 	try {
	 		QueryBuilder queryQange =conditionUtil(map);
	 		SearchIndexAction search = new SearchIndexAction(client,esBean).setQueryBuilder(queryQange).fetchSource(includeFields, null);
	 		list =search.sraech();		
	 		return list;
	 		 //请求超时，大概出问题了，初始化走一波
		 	} catch (ConnectTimeoutException  e) {
				Constant.cacheFalg=false;
				throw e;
			 
			}catch (ContentTooLongException e) {
				LOG.error("单次获取的的数据条数设置过大,请设置小一点:"+e);
				return list;
			}catch (IOException e) {
				//该异常不处理
				if(e.getMessage().contains("listener timeout after waiting")){
					LOG.error("listener timeout after waiting:"+e);
					return list;
				}else{
					throw e;
				}
			} catch (IllegalStateException e) {
				
				if(e.getMessage().contains("STOPPED")){
					Constant.cacheFalg=false;
					LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
					//throw e;
				}else{
					 throw e;
				 }
			}catch(NullPointerException e){
				//如果是客户端突然失效,那就重新初始化
				if(client!=null){
					LOG.error("ES运行异常,client 为 null"+e);
					Constant.cacheFalg=false;
				}else{
					LOG.error("参数异常"+e);
					throw e;
				}
			}
		return list;
	}
	@Override
	public  Map<String,Object> searchDocById(EsBean esBean,String idvalue,String... includeFields) throws Exception{
		Map<String,Object> map = new HashMap<String,Object>();
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		try {
	 		SearchIndexAction search = new SearchIndexAction(client,esBean).fetchSource(includeFields, null);
	 		map=search.sraechById(idvalue);
		} catch (IOException e) {
			//该异常不处理
			if(e.getMessage().contains("listener timeout after waiting")){
				LOG.error("listener timeout after waiting:"+e);
				return map;
			}else{
				throw e;
			}
		} catch (IllegalStateException e) {
				//连接关闭了,赶紧初始化打开,异常不抛出
			if(e.getMessage().contains("STOPPED")){
					Constant.cacheFalg=false;
					LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
			}
		}catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常"+e);
				throw e;
			}
		}
		return map;
	}
	@Override
	public EsBean searchDocByScroll(Map<String, Object> map, EsBean esBean,
			String... includeFields) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
	 	try {
	 		QueryBuilder queryQange =conditionUtil(map);
	 		SearchIndexAction search = new SearchIndexAction(client,esBean).setQueryBuilder(queryQange).fetchSource(includeFields, null);
	 		esBean =search.searchScroll();	
	 		return esBean;
	 		 //请求超时，大概出问题了，初始化走一波
		} catch (ConnectTimeoutException  e) {
				Constant.cacheFalg=false;
				throw e;
		}catch (ContentTooLongException e) {
				LOG.error("单次获取的的数据条数设置过大,请设置小一点:"+e);
				esBean.setDataList(new ArrayList<Map<String, Object>>());
				return esBean;
		}catch (IOException e) {
			//该异常不处理
			if(e.getMessage().contains("listener timeout after waiting")){
				LOG.error("listener timeout after waiting:"+e);
				esBean.setDataList(new ArrayList<Map<String, Object>>());
				return esBean;
			}
		} catch (IllegalStateException e) {
				//连接关闭了,赶紧初始化打开,异常不抛出
			if(e.getMessage().contains("STOPPED")){
					Constant.cacheFalg=false;
					LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
					//throw e;
			}else{
				 throw e;
			 }
		}catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常"+e);
				throw e;
			}
		}
		return esBean;
	}
	@Override
	public Long count(Map<String, Object> map, EsBean esBean) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		try {
			QueryBuilder queryQange =conditionUtil(map);
			SearchIndexAction search = new SearchIndexAction(client,esBean).setQueryBuilder(queryQange);
			Long count =search.count();
			return count;
	 		 //请求超时，大概出问题了，初始化走一波
		} catch (ConnectTimeoutException  e) {
				Constant.cacheFalg=false;
				throw e;
			 //响应超时
		}catch (IOException e) {
			//该异常不处理
			if(e.getMessage().contains("listener timeout after waiting")){
				LOG.error("listener timeout after waiting:"+e);
				return null;
			}else{
				throw e;
			}	
		}catch (IllegalStateException e) {
				//连接关闭了,赶紧初始化打开
			if(e.getMessage().contains("STOPPED")){
				Constant.cacheFalg=false;
				LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
			 }else{
				 throw e;
			 }
		}catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常"+e);
				throw e;
			}
		}
		return 0l;
	}

	@Override
	public boolean delDoc(Map<String, Object> map, EsBean esBean) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		
		try {
			QueryBuilder queryQange =conditionUtil(map);
			DelIndexAction delaction = new DelIndexAction(client,esBean.getIndex(),esBean.getIndex()).setQueryBuilder(queryQange);
			delaction.delDocByQuery();
	 		 //请求超时，大概出问题了，初始化走一波
		} catch (ConnectTimeoutException  e) {
				Constant.cacheFalg=false;
				throw e;
			 //响应超时
		}catch (IOException e) {
			//该异常不处理
			if(e.getMessage().contains("listener timeout after waiting")){
				LOG.error("listener timeout after waiting:"+e);
				return false;
			}else{
				throw e;
			}	
		}catch (IllegalStateException e) {
				//连接关闭了,赶紧初始化打开
			if(e.getMessage().contains("STOPPED")){
				Constant.cacheFalg=false;
				LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
			}else{
				 throw e;
			 }
		}catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常"+e);
				throw e;
			}
		}
		return false;
	}
	@Override
	public boolean delDocById(String idvalue, EsBean esBean) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		try{
		DelIndexAction delaction = new DelIndexAction(client,esBean.getIndex(),esBean.getIndex());
		return delaction.delDocById(idvalue);
		 //请求超时，大概出问题了，初始化走一波
	} catch (ConnectTimeoutException  e) {
			Constant.cacheFalg=false;
			throw e;
		 //响应超时
	}catch (IOException e) {
			//该异常不处理
			if(e.getMessage().contains("listener timeout after waiting")){
				LOG.error("listener timeout after waiting:"+e);
				return false;
			}else{
				throw e;
			}	
	}catch (IllegalStateException e) {
			//连接关闭了,赶紧初始化打开
			if(e.getMessage().contains("STOPPED")){
				Constant.cacheFalg=false;
				LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
			}
	}catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常"+e);
				throw e;
			}
	 }
	return false;
  }

	@Override
	public boolean existsDocById(String idvalue, EsBean esBean) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		try{
			SearchIndexAction search = new SearchIndexAction(client,esBean);
			return search.existsDocById(idvalue);
	 		 //请求超时，大概出问题了，初始化走一波
		}catch (IllegalStateException e) {
				//连接关闭了,赶紧初始化打开
			if(e.getMessage().contains("STOPPED")){
				Constant.cacheFalg=false;
				LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
			}else{
				 throw e;
			 }
		}catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常"+e);
				throw e;
			}
		}
		return false;
	}
	@Override
	public boolean clearScroll(String scrollId,EsBean esBean) throws Exception {
		if(esBean==null)throw new NullPointerException("esBean 不能为null");
		if(MyTools.isEmpty(scrollId))throw new NullPointerException("scrollId 不能为null或空");
		//检查连接是否正常,不正常重新初始化
		if(!inspectEsStatus(esBean.getIndex(),true))throw new EsInitMonException("ES重新初始化失败...当前状态:"+Constant.cacheFalg);
		try {
			SearchIndexAction search = new SearchIndexAction(client,esBean);
			return search.clearScroll(scrollId);
		} catch (ConnectTimeoutException  e) {
			Constant.cacheFalg=false;
			throw e;
		 //响应超时
		}catch (IOException e) {
			//该异常不处理
			if(e.getMessage().contains("listener timeout after waiting")){
				LOG.error("listener timeout after waiting:"+e);
				return false;
			}else{
				throw e;
			}	
		}catch (IllegalStateException e) {
			//连接关闭了,赶紧初始化打开
			if(e.getMessage().contains("STOPPED")){
				Constant.cacheFalg=false;
				LOG.error("ES运行异常,需要重新初始化,初始化失败则程序停止:"+e);
			}else{
				 throw e;
			 }
	  }catch(NullPointerException e){
			//如果是客户端突然失效,那就重新初始化
			if(client!=null){
				LOG.error("ES运行异常,client 为 null"+e);
				Constant.cacheFalg=false;
			}else{
				LOG.error("参数异常"+e);
				throw e;
			}
	  }
		return false;
	}
	/**
	 *检查ES的状态
	 * @param  index 索引库
	 * @param  isQuery 是否是查询  true：是查询    false：非查询操作
	 * @return 
	 * @throws Exception 
	 */
 private  boolean inspectEsStatus(String index,boolean isQuery) throws Exception{
	 	if(null==client)Constant.cacheFalg=false;
		boolean flag = true;
		 ESConfigInit es = new ESConfigInit();
		//ES需要重新初始化
		if(!Constant.cacheFalg){
			synchronized(this){
			  LOG.debug("ES开始重新初始化当前状态:"+Constant.cacheFalg);
			  //再次检查,双重锁
			  if(Constant.cacheFalg)return flag;
			    try {
				   Constant.cacheFalg =es.initEsConfig();
				   flag=Constant.cacheFalg;
				   if(flag)client=Constant.client.getRhlClient();
			  } catch (Exception e) {
				  flag=false;
				  throw e;
			   }
			}
		}
		//如果不存在,而且又是非查询的,注意:只有插入的时候才执行以下,如果是查询直接忽略该报错还是报错
		if(!es.isExistsIndex(index)&&flag&&!isQuery){
			flag=es.createIndex(index);
			LOG.debug("ES创建索引库:"+index+">>创建状态:"+flag);
		}
		return flag;
	}
}
