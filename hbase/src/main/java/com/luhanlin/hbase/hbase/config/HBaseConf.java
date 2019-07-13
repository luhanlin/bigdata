package com.luhanlin.hbase.hbase.config;

import com.luhanlin.hbase.hbase.spilt.SpiltRegionUtil;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author:
 * @description:
 * @Date:Created in 2018-03-29 22:15
 */
public class HBaseConf implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(HBaseConf.class);

    private static final String HBASE_SERVER_CONFIG = "hbase/hbase-server-config.properties";
    private static final String HBASE_SITE = "hbase/hbase-site.xml";
   // private static final String HDFS_SITE = "hbase/hdfs-site.xml";

    private volatile static HBaseConf hbaseConf;
    private CompositeConfiguration hbase_server_config;

    public CompositeConfiguration getHbase_server_config() {

        return hbase_server_config;
    }

    public void setHbase_server_config(CompositeConfiguration hbase_server_config) {
        this.hbase_server_config = hbase_server_config;
    }

    //hbase 配置文件
    private  Configuration configuration;
    //hbase 连接
    private volatile transient Connection conn;


    /**
     * 初始化HBaseConf的时候加载配置文件
     */
    private HBaseConf() {
        hbase_server_config = new CompositeConfiguration();
        //加载配置文件
        loadConfig(HBASE_SERVER_CONFIG,hbase_server_config);
        //初始化连接
        getHconnection();
    }

    //获取连接
    public Configuration getConfiguration(){
        if(configuration==null){
            configuration = HBaseConfiguration.create();
            configuration.addResource(HBASE_SITE);
            LOG.info("加载配置文件" + HBASE_SITE + "成功");
        }
        return configuration;
    }

    public BufferedMutator getBufferedMutator(String tableName) throws IOException {
        return getHconnection().getBufferedMutator(TableName.valueOf(tableName));
    }


    public Connection getHconnection(){

        if(conn==null){
            //获取配置文件
            getConfiguration();
            synchronized (HBaseConf.class) {
                if (conn == null) {
                    try {
                        conn = ConnectionFactory.createConnection(configuration);
                    } catch (IOException e) {
                        LOG.error(String.format("获取hbase的连接失败  参数为： %s", toString()), e);
                    }
                }
            }
        }
        return conn;
    }


    /**
     * 加载配置文件
     * @param path
     * @param configuration
     */
    private void loadConfig(String path,CompositeConfiguration configuration) {
        try {
            LOG.info("加载配置文件 " + path);
            configuration.addConfiguration(new PropertiesConfiguration(path));
            LOG.info("加载配置文件" + path +"成功。 ");
        } catch (ConfigurationException e) {
            LOG.error("加载配置文件 " + path + "失败", e);
        }
    }


    /**
     * 单例 初始化HBaseConf
     * @return
     */
    public static HBaseConf getInstance() {
        if (hbaseConf == null) {
            synchronized (HBaseConf.class) {
                if (hbaseConf == null) {
                    hbaseConf = new HBaseConf();
                }
            }
        }
        return hbaseConf;
    }


    public static void main(String[] args) {
        String hbase_table = "test:chl_test2";
        HBaseTableUtil.createTable(hbase_table, "cf", true, -1, 1, SpiltRegionUtil.getSplitKeysBydinct());

      /*  Connection hconnection = HBaseConf.getInstance().getHconnection();
        Connection hconnection1 = HBaseConf.getInstance().getHconnection();
        System.out.println(hconnection);
        System.out.println(hconnection1);*/
    }
}
