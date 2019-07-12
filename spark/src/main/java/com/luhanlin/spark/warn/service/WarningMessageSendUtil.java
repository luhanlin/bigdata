package com.luhanlin.spark.warn.service;

import com.bigdata.common.regex.Validation;
import com.luhanlin.spark.warn.domain.WarningMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-04-25 06:10
 */
public class WarningMessageSendUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WarningMessageSendUtil.class);

    public static void messageWarn(WarningMessage warningMessage) {

        String[] mobiles = warningMessage.getSendMobile().split(",");

        for(String phone:mobiles){
            if(Validation.isMobile(phone)){
                System.out.println("开始向手机号为" + phone + "发送告警消息====" + warningMessage);
                StringBuffer sb= new StringBuffer();
                String content=warningMessage.getSenfInfo().toString();
                //TODO  调用短信接口发送消息
                //TODO  怎么通过短信发送  这个是需要公司开通接口
                //TODO  DINGDING
                // 专门的接口
             /*   sb.append(ClusterProperties.https_url + "username=" + ClusterProperties.https_username +
                        "&password=" + ClusterProperties.https_password + "&mobile=" + phone +
                        "&apikey=" + ClusterProperties.https_apikey+
                        "&content=" + URLEncoder.encode(content));*/
               // sendMessage(sb.toString());
            }
        }
    }


}
