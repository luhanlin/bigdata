package com.luhanlin.redis.client;

import com.bigdata.common.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Properties;

/**
 * @author:
 * @description:
 * @Date:Created in 2018-05-21 14:14
 */
public class JedisSingle {

    private static final Logger LOG = LoggerFactory.getLogger(JedisSingle.class);
    private static Properties redisConf;

    /**
     * 读取redis配置文件
     *redis.hostname = 192.168.247.103
     *redis.port  = 6379
     */
    static {
        redisConf = ConfigUtil.getInstance().getProperties("redis/redis.properties");
        System.out.println(redisConf);
    }

    public static Jedis getJedis(int db){
        Jedis jedis = JedisSingle.getJedis();
        if(jedis!=null){
            jedis.select(db);
        }
        return jedis;
    }

    public static void main(String[] args) {
        Jedis jedis = JedisSingle.getJedis(15);
        Map<String, String> Map = jedis.hgetAll("phone:18609765435");
        System.out.println(Map.toString());
    }


    /**
     *
     * @return
     */
    public static Jedis getJedis(){
        int timeoutCount = 0;
        while (true) {// 如果是网络超时则多试几次
            try
            {
                 Jedis jedis = new Jedis(redisConf.get("redis.hostname").toString(),
                         Integer.valueOf(redisConf.get("redis.port").toString()));
                return jedis;
            } catch (Exception e)
            {
                if (e instanceof JedisConnectionException || e instanceof SocketTimeoutException)
                {
                    timeoutCount++;
                    LOG.warn("获取jedis连接超时次数:" +timeoutCount);
                    if (timeoutCount > 4)
                    {
                        LOG.error("获取jedis连接超时次数a:" +timeoutCount);
                        LOG.error(null,e);
                        break;
                    }
                }else
                {
                    LOG.error("getJedis error", e);
                    break;
                }
            }
        }
        return null;
    }

    public static void close(Jedis jedis){
        if(jedis!=null){
            jedis.close();
        }
    }
}
