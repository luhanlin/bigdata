package com.luhanlin.hbase.hbase.config;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class HBaseTableFactory implements Serializable {

	private static final long serialVersionUID = -1071596337076137201L;

	private static final Logger LOG = LoggerFactory.getLogger(HBaseTableFactory.class);

	private HBaseConf conf;
	private transient Connection conn  ;
	private boolean isReady = true;

	public HBaseTableFactory(){

		conf = HBaseConf.getInstance();
		if(true){
			conn = conf.getHconnection();
		}else{
			isReady = false;
			LOG.warn("HBase 连接没有启动。");
		}
	}

	public HBaseTableFactory(Connection conn){
		this.conn = conn;
	}

	/**
	  * description:  根据表名创建 表的实例
	  * @param tableName
	  * @return
	  * @throws IOException
	  * HTableInterface
	  * 2015-10-7 上午10:17:04
	  * by
	 */
	public Table getHBaseTableInstance(String tableName) throws IOException{

		if(conn == null){
			if(conf == null){
				conf = HBaseConf.getInstance();
				isReady = true;
				LOG.warn("HBaseConf为空，重新初始化。");
			}
			synchronized (HBaseTableFactory.class) {
				if(conn == null) {
					conn = conf.getHconnection();
					LOG.warn("初始 hbase Connection 为空 ， 获取  Connection成功。");
				}
			}
		}
		return  isReady ? conn.getTable(TableName.valueOf(tableName)) : null;
	}

	public HTable getHTable(String tableName) throws IOException{

		return  (HTable) getHBaseTableInstance(tableName);
	}

	public BufferedMutator getBufferedMutator(String tableName) throws IOException {
		return getConf().getBufferedMutator(tableName);
	}

	public boolean isReady() {
		return isReady;
	}

	private HBaseConf getConf(){
		if(conf == null){
			conf = HBaseConf.getInstance();
		}
		return conf;
	}

	public void close() throws IOException{

		conn.close();
		conn = null;
	}

}
