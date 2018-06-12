package com.montnets.input_jdbc_es.util;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**  
 * @Title:  NetAddrUtil.java   
 * @Description: 从url中分离ip和port
 * @author: chenhongjie     
 * @date:   2018年5月25日 下午5:17:34   
 * @version V1.0 
 */
public class NetAddrUtil {
	
	 private String ip;
	 private Integer port;
      
    /** 
     * 从url中分析出hostIP:PORT<br/> 
     * @param url 
     * */  
    public  void IpPortFromUrl(String url) {  

        String host = "";  
        Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+(:\\d{0,5})?");  
        Matcher matcher = p.matcher(url);  
        if (matcher.find()) {  
            host = matcher.group() ;  
        }  
        // 如果  
        if(host.contains(":") == false){  
        	this.ip=host;
        	this.port=80;
        }else{ 
        	String[] ipPortArr = host.split(":");  
        	this.ip=ipPortArr[0];
        	this.port=Integer.valueOf(ipPortArr[1].trim());
        }
    }  
      
    public String getIp() {
		return ip;
	}

	public Integer getPort() {
		return port;
	}

	public static void main(String [] args){  
        String url = "10.33.32.81:8080";  
        NetAddrUtil addr = new  NetAddrUtil();  
        addr.IpPortFromUrl(url);
       // IpPortAddr addr= NetAddrUtil.getIpPortFromUrl(url) ;  
        System.out.println(addr.getIp() +"=========>" +addr.getPort() );  
    }  
}
