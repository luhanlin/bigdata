package com.bigdata.flume.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Description:
 *
 * @author
 * @version 1.0
 * @date 2017/3/3 10:04
 */

public class ClusterProperties {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigurationFileReader.class);
    public static ClusterConfigurationFileReader clusterReader = ClusterConfigurationFileReader.getInstance();
    public static String es_nodes;
    public static String es_port;
    public static String es_port2;
    public static String es_clustername;
    public static String es_clustername2;
    public static String es_user;


    //ES索引
    public static String stationcenterIndexPrefix;
    public static String apcenterIndexPrefix;
    public static String imsiimeiIndexPrefix;

    public static String wifiIndex;
    public static String virtualIndex;

    public static List<String> wifiType;
    public static List<String> virtualType;

    // redis配置
    public static String redisHost;
    public static int redisPort;

    //oracle配置
    public static String connstring;
    public static String username;
    public static String password;
    public static String username1;
    public static String password1;

    public static String dzwlimsiimeiIndex;
    public static String dzwlvirtualinfoIndex;
    public static String dzwlwifiIndex;


    static {
        try {
            es_nodes = clusterReader.getFields("es.nodes").toString().replace("[", "").replace("]", "").replace(" ", "");
            es_port = clusterReader.getKey("es.port");
            es_port2 = clusterReader.getKey("es.port2");

            es_clustername = clusterReader.getKey("es.clustername");
            es_clustername2 = clusterReader.getKey("es.clustername2");
            es_user = clusterReader.getKey("es.user");

            stationcenterIndexPrefix = clusterReader.getKey("stationcenterIndexPrefix");
            apcenterIndexPrefix = clusterReader.getKey("apcenterIndexPrefix");
            imsiimeiIndexPrefix = clusterReader.getKey("imsiimeiIndexPrefix");

            wifiIndex = clusterReader.getKey("wifiIndex");
            virtualIndex = clusterReader.getKey("virtualIndex");

            wifiType = clusterReader.getFields("wifitype");
            virtualType = clusterReader.getFields("virtualtype");

        } catch (Exception e) {
            LOG.error("加载ES集群信息失败", e);
        }

        try {
            connstring = clusterReader.getKey("oracle.connstring");
            username = clusterReader.getKey("oracle.username");
            password = clusterReader.getKey("oracle.password");
            username1 = clusterReader.getKey("oracle.username1");
            password1 = clusterReader.getKey("oracle.password1");

            dzwlimsiimeiIndex = clusterReader.getKey("dzwlimsiimeiIndex");
            dzwlvirtualinfoIndex = clusterReader.getKey("dzwlvirtualinfoIndex");
            dzwlwifiIndex = clusterReader.getKey("dzwlwifiIndex");
        } catch (Exception e) {
            LOG.error("加载oracle信息失败", e);
        }

        try {
            redisHost = clusterReader.getKey("redis.host");
            redisPort = clusterReader.getKeyInt("redis.port");
        } catch (Exception e) {

        }
    }

    public static void main(String args[]) {
        System.out.println(ClusterProperties.es_nodes);
    }

}
