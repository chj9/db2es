package com.montnets.input_jdbc_es.db.impl;



import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.montnets.input_jdbc_es.db.ConnectionManager;
import com.montnets.input_jdbc_es.db.DBDao;
import com.montnets.input_jdbc_es.util.Constant;

/**
 * 
 * Title: DBDaoImpl 
 * Description:
 * Mysql数据库操作实现类 
 * Version:1.0.0
 * @author pancm
 * @date 2018年1月4日
 */
public class DBDaoImpl implements DBDao {
	private static Logger LOG = LoggerFactory.getLogger(DBDaoImpl.class);
	private Statement statement =null;
	private ResultSet rs = null;
	/***
	 * 获取数据库连接
	 * @return
	 * @throws SQLException 
	 */
	private Connection getConnection() throws SQLException {
		return  ConnectionManager.getInstance().getConnection();
	}

	@Override
	public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
		List<Map<String, Object>> list = null;
		try {
		 rs = executeQuerySQL(sql);
		 list =convertList(rs);
	     rs.close();
		} catch (SQLException e) {
			LOG.error("数据库查询报错",e);
			throw e;
		}
		return list;
	}
	/**
	 * 获取表行总数
	 */
	@Override
	public Long tableCount(String sql) throws SQLException{
		//String sql =String.format("select count(*) AS counts from %s", tableName);
		try {
			 rs = executeQuerySQL(sql);
			 Long rowCount  = 0l;
	          while(rs.next()){  
	                rowCount = rs.getLong(1);  
	            }  
			 return rowCount;
		} catch (Exception e) {
			LOG.error("数据库查表总数报错",e);
			throw e;
		}
	}
    /**
	 * 将查询的数据转换成List类型
	 * 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private  List<Map<String, Object>> convertList(ResultSet rs) throws SQLException {
		if (null == rs) {
			return null;
		}
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		ResultSetMetaData md = rs.getMetaData();
		int columnCount = md.getColumnCount();
		while (rs.next()) {
			Map<String, Object> rowData = new HashMap<String, Object>();
			for (int i = 1; i <= columnCount; i++) {
				//因为ES需要把所有参数名转为小写
				rowData.put(md.getColumnName(i).toLowerCase(), rs.getObject(i));
			}
			list.add(rowData);
		}
		return list;
	}
	private ResultSet executeQuerySQL(String sql) throws SQLException{
		if(Constant.CONNECTION==null){
			Constant.CONNECTION = getConnection(); 
		}
		if(statement==null){
	        statement = Constant.CONNECTION.createStatement();
		}
		rs = statement.executeQuery(sql);
		return rs;
	}
	public static void main(String[] args) throws SQLException {
		ResultSet result = new DBDaoImpl().executeQuerySQL("");
		while (result.next()) {
			System.out.println(result.getLong(1));
			break;
		}
	}
}
