package com.montnets.input_jdbc_es.service;


import java.util.List;
import java.util.Map;

import com.montnets.input_jdbc_es.bean.EsBean;

/**
 * 
* Copyright: Copyright (c) 2018 Montnets
* 
* @ClassName: IEsService.java
* @Description: 该类的功能描述
*es接口
* @version: v1.0.0
* @author: chenhj
* @date: 2018年6月6日 下午3:18:55 
*
* Modification History:
* Date         Author          Version            Description
*---------------------------------------------------------*
* 2018年6月6日     chenhj          v1.0.0               修改原因
 */
public interface IEsService {
    /**
     * ES保存数据方法
     * @param list 数据集
     * @param esBean es目标库
     */
	public boolean saveDoc(List<Map<String,Object>> list,EsBean esBean) throws Exception;
    /**
     * ES查询数据方法
     *注意：如果是区间查询,<B>大于gt,大于等于gte,小于lt,小于等于lte</B>
     * <P>如根据参数time的区间查询:
     * map.put("time","{"gt":"2018-05-06","lt":"2018-05-25"}")
     *对数字的区间查询,如根据参数num的区间查询：
     * map.put("num","{"gt":1000,"lt":2000}")
     * 查询是否存在某个参数名请这样做,其中key为命令,value为参数名
     * <B>existsMustNotQuery：这个字段一定不存在,existsMustQuery：这个字段一定存在</B>
     *map.put("existsMustNotQuery","field1,field2,field3...")
     * map.put("existsMustQuery","field1,field2,field3...")
     * @param map 条件集,无条件可为null
     * @param esBean es目标库
     * @param includeFields 需要的参数字段,如果全需要全部字段无需设置
     * @return 返回数据集合
     */
	public  List<Map<String,Object>> searchDoc(Map<String,Object> map,EsBean esBean,String ...includeFields) throws Exception;
	/**
	 * 根据ID获取文档
     * @param esBean  es目标库
     * @param idvalue ID值
     * @param includeFields 所需字段值,需要全部字段则无需填写
	 */
	public  Map<String,Object> searchDocById(EsBean esBean,String idvalue,String... includeFields) throws Exception;
    /**
     * 根据游标滚动获取数据
     * 注意：如果是区间查询,<B>大于gt,大于等于gte,小于lt,小于等于lte</B>
     * <P>如根据参数time的区间查询:
     *map.put("time","{"gt":"2018-05-06","lt":"2018-05-25"}")
     * 对数字的区间查询,如根据参数num的区间查询：
     * map.put("num","{"gt":1000,"lt":2000}")
     * 查询是否存在某个参数名请这样做,其中key为命令,value为参数名
     * <B>existsMustNotQuery：这个字段一定不存在,existsMustQuery：这个字段一定存在</B>
     * map.put("existsMustNotQuery","field1,field2,field3...")
     * map.put("existsMustQuery","field1,field2,field3...")
     * @param map 条件集,无条件可为null
     * @param esBean ES配置
     * @param includeFields 需要的参数字段,如果全需要全部字段无需设置
     * @return 返回数据集合
     */
    public  EsBean searchDocByScroll(Map<String,Object> map,EsBean dataPage,String ...includeFields) throws Exception;	
    /**
     * ES查询总数方法
     * 注意：如果是区间查询,<B>大于gt,大于等于gte,小于lt,小于等于lte</B>
     * <P>如根据参数time的区间查询:
     * map.put("time","{"gt":"2018-05-06","lt":"2018-05-25"}")
     * 对数字的区间查询,如根据参数num的区间查询：
     * map.put("num","{"gt":1000,"lt":2000}")
     *<查询是否存在某个参数名请这样做,其中key为命令,value为参数名
     * <B>existsMustNotQuery：这个字段一定不存在,existsMustQuery：这个字段一定存在</B>
     * map.put("existsMustNotQuery","field1,field2,field3...")
     * map.put("existsMustQuery","field1,field2,field3...")
     * @param map 条件集,无条件可为null
     * @param esBean es目标库
     * @return 返回数据条数
     */
	public  Long count(Map<String,Object> map,EsBean esBean) throws Exception;
    /**
     * ES删除方法
     * 注意：如果是区间删除,<B>大于gt,大于等于gte,小于lt,小于等于lte</B>
     * <P>如根据参数time的区间删除:
     * map.put("time","{"gt":"2018-05-06","lt":"2018-05-25"}")
     * 对数字的区间删除,如根据参数num的区间删除：
     * map.put("num","{"gt":1000,"lt":2000}")
     *<查询是否存在某个参数名请这样做,其中key为命令,value为参数名
     * existsMustNotQuery：这个字段一定不存在,existsMustQuery：这个字段一定存在</B>
     * map.put("existsMustNotQuery","field1,field2,field3...")
     * map.put("existsMustQuery","field1,field2,field3...")
     * @param map 条件集,无条件可为null
     * @param esBean es目标库
     * @return 返回删除状态
     */
	public  boolean delDoc(Map<String,Object> map,EsBean esBean) throws Exception;

    /**
     * ES根据ID删除数据
     * @param idvalue ID
     * @param esBean es目标库
     * @return 返回删除状态
     */
	public  boolean delDocById(String idvalue,EsBean esBean) throws Exception;
    /**
     * ES根据ID查询内容是否存在
     * @param idvalue ID
     * @param esBean es目标库
     * @return 返回查询状态
     */
	public  boolean existsDocById(String idvalue,EsBean esBean) throws Exception;
	
    /**
     * 释放游标资源
     * @param scrollId 游标ID
     * @return 返回状态
     */
	public  boolean clearScroll(String scrollId,EsBean esBean) throws Exception;
	
	
	
}
