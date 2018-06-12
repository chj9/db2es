package com.montnets.input_jdbc_es.db;


import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.montnets.input_jdbc_es.util.Constant;
import com.montnets.input_jdbc_es.util.GetProperties;

/**
 * 
 * Title: ConnectionManager
 * Description:Mysql数据库连接初始类
 * Version:1.0.0
 * @author pancm
 * @date 2018年1月4日
 */
public class ConnectionManager {
	private static Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

	/** 定义c3p0 连接池的数据源 */
	private static ComboPooledDataSource cpds=null;
	private static volatile ConnectionManager dbConnection;
	
	/**
	 * 在构造函数初始化的时候获取数据库连接
	 */
	private ConnectionManager() {
		/** 获取jdbc配置文件参数 */
	    Map<String, String> conf =new GetProperties(Constant.GLOBAL_CONFIG_NAME).getAppSettings();
		/** 获取属性文件中的值 **/
		String driverName = conf.get("driverClassName");
		String url = conf.get("url");
		String username = conf.get("username");
		String password = conf.get("password");	
			try {
				/** 数据库连接池对象 **/
				cpds = new ComboPooledDataSource();
				/** 设置数据库连接驱动 **/
				cpds.setDriverClass(driverName);
				/** 设置数据库连接地址 **/
				cpds.setJdbcUrl(url);
				/** 设置数据库连接用户名 **/
				cpds.setUser(username);
				/** 设置数据库连接密码 **/
				cpds.setPassword(password);
				/** 如果设为true那么在取得连接的同时将校验连接的有效性。Default: false **/
				cpds.setTestConnectionOnCheckin(true);
				/** 定义在从数据库获取新的连接失败后重复尝试获取的次数，默认为10; **/
				cpds.setAcquireRetryAttempts(10);
				/** 两次连接中间隔时间默认为1000毫秒 **/
				cpds.setAcquireRetryDelay(1000);
				/**
				 * 获取连接失败将会引起所有等待获取连接的线程异常,
				 * 但是数据源仍有效的保留,并在下次调用getConnection()的时候继续尝试获取连接.如果设为true,
				 * 那么尝试获取连接失败后该数据源将申明已经断开并永久关闭.默认为false
				 **/
				cpds.setBreakAfterAcquireFailure(false);
			} catch (PropertyVetoException e) {
				LOG.error("初始化参数失败！", e);
//				System.exit(1);
			}
		}

	/**
	 * 获取数据库连接对象，单例
	 * 
	 * @return
	 */
	public static ConnectionManager getInstance() {
		if (dbConnection == null) {
			synchronized (ConnectionManager.class) {
				if (dbConnection == null) {
					dbConnection = new ConnectionManager();
				}
			}
		}
		return dbConnection;
	}

	/**
	 * 获取数据库连接
	 * 
	 * @return 数据库连接
	 * @throws SQLException
	 */
	public final synchronized Connection getConnection() throws SQLException {
		return cpds.getConnection();
	}
	public static void close(ResultSet rs, Statement stmt, Connection connection) {
		try {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			LOG.error("数据连接关闭失败！", e);
		}
	}
}
