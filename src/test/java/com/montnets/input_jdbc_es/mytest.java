/**
 * 
 */
package com.montnets.input_jdbc_es;

import java.util.Map;

import com.montnets.input_jdbc_es.util.Constant;
import com.montnets.input_jdbc_es.util.GetProperties;

/**   
* Copyright: Copyright (c) 2018 Montnets
* 
* @ClassName: mytest.java
* @Description: 该类的功能描述
*
* @version: v1.0.0
* @author: chenhj
* @date: 2018年6月9日 下午2:04:43 
*
* Modification History:
* Date         Author          Version            Description
*---------------------------------------------------------*
* 2018年6月9日     chenhj          v1.0.0               修改原因
*/
public class mytest {
public static void main(String[] args) {
	Map<String, String>   conf=new GetProperties(Constant.GLOBAL_CONFIG_NAME).getAppSettings();
	String ismulti = conf.get("multi_table");
	 System.out.println(ismulti);
	boolean multi_table = Boolean.valueOf(ismulti.trim());
     System.out.println(multi_table);
}
}
