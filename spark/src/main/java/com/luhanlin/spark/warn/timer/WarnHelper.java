package com.luhanlin.spark.warn.timer;


import com.luhanlin.redis.client.JedisSingle;
import com.luhanlin.spark.warn.dao.TZ_RuleDao;
import com.luhanlin.spark.warn.domain.TZ_RuleDomain;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-04-25 01:08
 */
public class WarnHelper {

    private static final Logger LOG = LoggerFactory.getLogger(WarnHelper.class);
    /**
     * 同步mysql规则数据到redis
     */
    public static void syncRuleFromMysql2Redis(){
        //获取所有的规则
        List<TZ_RuleDomain> ruleList = TZ_RuleDao.getRuleList();
        Jedis jedis = null;
        try {
            //获取redis 客户端
            jedis = JedisSingle.getJedis(15);
            for (int i = 0; i < ruleList.size(); i++) {
                TZ_RuleDomain rule = ruleList.get(i);
                String id = rule.getId()+"";
                String publisher = rule.getPublisher();
                String warn_fieldname = rule.getWarn_fieldname();
                String warn_fieldvalue = rule.getWarn_fieldvalue();
                String send_mobile = rule.getSend_mobile();
                String send_type = rule.getSend_type();
                //拼接redis key值
                String redisKey = warn_fieldname +":" + warn_fieldvalue;
                //通过redis hash结构   hashMap
                jedis.hset(redisKey,"id",StringUtils.isNoneBlank(id) ? id : "");
                jedis.hset(redisKey,"publisher",StringUtils.isNoneBlank(publisher) ? publisher : "");
                jedis.hset(redisKey,"warn_fieldname",StringUtils.isNoneBlank(warn_fieldname) ? warn_fieldname : "");
                jedis.hset(redisKey,"warn_fieldvalue",StringUtils.isNoneBlank(warn_fieldvalue) ? warn_fieldvalue : "");
                jedis.hset(redisKey,"send_mobile",StringUtils.isNoneBlank(send_mobile) ? send_mobile : "");
                jedis.hset(redisKey,"send_type",StringUtils.isNoneBlank(send_type) ? send_type : "");
            }
        } catch (Exception e) {
           LOG.error("mysql同步规则到redis失败",e);
        } finally {
            JedisSingle.close(jedis);
        }
    }


    public static void main(String[] args)
    {
        WarnHelper.syncRuleFromMysql2Redis();
    }

}
