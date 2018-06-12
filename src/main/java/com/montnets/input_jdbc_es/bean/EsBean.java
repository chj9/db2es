package com.montnets.input_jdbc_es.bean;


import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**  
 * @Title:  EsBean.java   
 * @Description:  ES索引库属性
 * @author: chenhongjie     
 * @date:   2018年5月25日 上午11:40:13   
 * @version V1.0 
 */
public class EsBean implements Serializable{
	
	private static final long serialVersionUID = 1L;
	/***索引库***/
	private String index;
	/***索引库ID键***/
	private String idkey;
	/***当前页****/
	private int pageNo = 1;//
	/***每页数据****/
	private int pageSize = 1000;//
	/***是否需要分页,默认禁止分页****/
	private boolean needPaging = false;//
	/*****滚动取数据的时候单次取多少,默认一次1000*******/
	private int limit=1000;
	/*****响应数据集*******/
	private List<Map<String, Object>> dataList = null;//响应数据集
	private long totalCount = -1L;
	/*****滚动数据时候产生的游标ID*******/
	private String scrollId;
   
	
	
	public List<Map<String, Object>> getDataList() {
		return dataList;
	}
	public void setDataList(List<Map<String, Object>> dataList) {
		this.dataList = dataList;
	}
	public long getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}
	public String getScrollId() {
		return scrollId;
	}
	public void setScrollId(String scrollId) {
		this.scrollId = scrollId;
	}
	public int getPageNo() {
		return pageNo;
	}
	public int getLimit() {
		return limit;
	}
	public void setLimit(int limit) {
		this.limit = limit;
	}
	public EsBean(String index){
			   this.index=index;
	}
	public EsBean(String index,String idkey){
			   this.index=index;
			   this.idkey=idkey;
	}	
	public String getIdkey() {
			return idkey;
	}
	public void setIdkey(String idkey) {
			this.idkey = idkey;
	}
	public String getIndex() {
			return index;
	}
	public void setIndex(String index) {
			this.index = index;
	}
	public void setPageNo(int pageNo){
		    this.pageNo = pageNo;
		    if (pageNo < 1)this.pageNo = 1;
	}
	 public int getPageSize(){
		 return this.pageSize;
	}
	public void setPageSize(int pageSize){
		    this.pageSize = pageSize;
	}
	  public int getStartIndex() {
			    return getFirst();
	}
	  public int getFirst(){
		    return (this.pageNo - 1) * this.pageSize;
	}
	public boolean isNeedPaging() {
			return needPaging;
	}
	public void setNeedPaging(boolean needPaging) {
			this.needPaging = needPaging;
	}
}
