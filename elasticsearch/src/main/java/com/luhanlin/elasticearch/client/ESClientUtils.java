package com.luhanlin.elasticearch.client;


import com.bigdata.common.config.ConfigUtil;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Properties;

/**
 * @author:
 * @description: ES 客户端获取
 * @Date:Created in 2018-04-03 15:48
 */
public class ESClientUtils implements Serializable{

    private static Logger LOG = LoggerFactory.getLogger(ESClientUtils.class);
    private volatile static TransportClient esClusterClient;
    private ESClientUtils(){}
    private static Properties properties;
    static {
        properties = ConfigUtil.getInstance().getProperties("es/es_cluster.properties");
    }

    public static TransportClient getClient(){
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        String clusterName = properties.getProperty("es.cluster.name");
        String clusterNodes1 = properties.getProperty("es.cluster.nodes1");
        String clusterNodes2 = properties.getProperty("es.cluster.nodes2");
        String clusterNodes3 = properties.getProperty("es.cluster.nodes3");
        LOG.info("clusterName:"+ clusterName);
        LOG.info("clusterNodes:"+ clusterNodes1);
        LOG.info("clusterNodes:"+ clusterNodes2);
        LOG.info("clusterNodes:"+ clusterNodes3);
        if(esClusterClient==null){
            synchronized (ESClientUtils.class){
                if(esClusterClient==null){
                    try{
                        Settings settings = Settings.builder()
                                .put("cluster.name", clusterName)
                                .put("client.transport.sniff",true).build();//开启自动嗅探功能
                        esClusterClient = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(clusterNodes1), 9300))
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(clusterNodes2), 9300))
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(clusterNodes3), 9300));
                        LOG.info("esClusterClient========" + esClusterClient.listedNodes());
                    }catch (Exception e){
                        LOG.error("获取客户端失败",e);
                    }finally {

                    }
                }
            }
        }
        return esClusterClient;
    }


    public static void main(String[] args) {
        TransportClient client = ESClientUtils.getClient();
        System.out.println(client);
    }
}
