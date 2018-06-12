package com.montnets.input_jdbc_es.db;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**  
 * @Title:  DBDao.java   
 * @Description:  TODO(用一句话描述该文件做什么)   
 * 数据库接口
 * @author: chenhongjie     
 * @date:   2018年4月10日 下午2:26:13   
 * @version V1.0 
 */
public interface DBDao {
	public List<Map<String, Object>> executeQuery(String sqls)throws SQLException;
	public Long tableCount(String sql)throws SQLException;
}
