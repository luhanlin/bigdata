
package com.luhanlin.spark.warn.service;


import com.luhanlin.spark.warn.dao.WarningMessageDao;
import com.luhanlin.spark.warn.domain.WarningMessage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Description:
 *
 * @author
 * @version 1.0
 * @date 2017/5/11 12:49
 */


public class BlackRuleWarning {
    private static final Logger LOG = LoggerFactory.getLogger(BlackRuleWarning.class);
    //可以通过数据库，配置文件加载

    //为了遍历所有预警字段
    private static List<String> listWarnFields = new ArrayList<>();
    static {
        listWarnFields.add("phone");
        listWarnFields.add("mac");
    }


    /**
     * 预警流程处理
     * @param map
     * @param jedis15
     */
    public static void blackWarning(Map<String, Object> map, Jedis jedis15) {

        listWarnFields.forEach(warnField -> {
            if (map.containsKey(warnField) && StringUtils.isNotBlank(map.get(warnField).toString())) {
                //获取预警字段核预警值  相当于手机号
                String warnFieldValue = map.get(warnField).toString();
                //去redis中进行比对
                //数据中  通过   "字段" + "字段值" 去拼接key
                //            phone       :    186XXXXXX
                String key = warnField + ":" + warnFieldValue;
                //redis中的key是   phone:18609765435
                System.out.println("拼接数据流中的key=======" + key);
                if (jedis15.exists(key)) {
                    //对比命中之后 就可以发送消息提醒
                    System.out.println("命中REDIS中的" + key + "===========开始预警");
                    beginWarning(jedis15, key);
                } else {
                    //直接过
                    System.out.println("未命中" + key + "===========不进行预警");
                }
            }
        });
    }


    /**
     *  规则已经命中，开始预警
     * @param jedis15
     * @param key
     */
    private static void beginWarning( Jedis jedis15, String key) {

        System.out.println("============MESSAGE -1- =========");
        //封装告警  信息及告警消息
        WarningMessage warningMessage = getWarningMessage(jedis15, key);


        System.out.println("============MESSAGE -4- =========");
        if (warningMessage != null) {
            //将预警信息写入预警信息表
            WarningMessageDao.insertWarningMessageReturnId(warningMessage);
            //String accountid = warningMessage.getAccountid();
            //String readAccounts = warningMessage.getAlarmaccounts();
            // WarnService.insertRead_status(messageId, accountid);
            if (warningMessage.getSendType().equals("2")) {
                //手机短信告警 默认告警方式
                WarningMessageSendUtil.messageWarn(warningMessage);
            }
        }

    }


    /**
     * 封装告警信息及告警消息
     * @param jedis15
     * @param key
     * @return
     */
    private static WarningMessage getWarningMessage(Jedis jedis15, String key) {
        System.out.println("============MESSAGE -2- =========");
        //封装消息
        String[] split = key.split(":");
        if (split.length == 2) {
            WarningMessage warningMessage = new WarningMessage();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String time = format.format(new Date());
            String clew_type = split[0];//告警字段
            String rulecontent = split[1];//告警字段值

            //从redis中获取消息信息进行封装
            Map<String, String> valueMap = jedis15.hgetAll(key);
            //规则ID (是哪条规则命中的)
            warningMessage.setAlarmRuleid(valueMap.get("id"));
            //预警方式
            warningMessage.setSendType(valueMap.get("send_type"));//告警方式，0：界面 1：邮件 2：短信 3：邮件+短信
            //预警信息接收手机号
            warningMessage.setSendMobile(valueMap.get("send_mobile"));
            //arningMessage.setSendEmail(valueMap.get("sendemail"));
            /*arningMessage.setAlarmaccounts(valueMap.get("alarmaccounts"));*/
            //规则发布人
            warningMessage.setAccountid(valueMap.get("publisher"));
            warningMessage.setAlarmType("2");
            StringBuffer warn_content = new StringBuffer();
            //预警内容 信息   时间  地点  人物
            //预警字段来进行设置  phone
            //我们有手机号


            //数据关联
            // 手机  MAC  身份证， 车牌  人脸。。URL 姓名
            // 全部设在推送消息里面
            warn_content.append("【网络告警】：手机号为:" + "[" + rulecontent + "]在时间" + time + "出现在" + ">附近,设备号"
            );
            String content = warn_content.toString();
            warningMessage.setSenfInfo(content);
            System.out.println("============MESSAGE -3- =========");
            return warningMessage;
        } else {
            return null;
        }

    }


}
