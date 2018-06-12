package com.montnets.input_jdbc_es.restore.exception;

/**
 * 自定义ES初始化异常
 * @Title:  EsInitMonException.java   
 * @Description:  TODO(用一句话描述该文件做什么)   
 * @author: chenhongjie     
 * @date:   2018年5月29日 上午10:26:14   
 * @version V1.0
 */
public  class EsInitMonException  extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /**
         * 错误编码
         */
        private String errorCode;

        /**
         * 构造一个基本异常.
         *
         * @param message
         *            信息描述
         */
        public EsInitMonException(String message)
        {
            super(message);
        }
        /**
         * 构造一个基本异常.
         *
         * @param message
         *            信息描述
         * @param cause
         *            根异常类（可以存入任何异常）
         */
        public EsInitMonException(String message, Throwable cause)
        {
            super(message, cause);
        }
        
        public String getErrorCode()
        {
            return errorCode;
        }

        public void setErrorCode(String errorCode)
        {
            this.errorCode = errorCode;
        }
        
}
