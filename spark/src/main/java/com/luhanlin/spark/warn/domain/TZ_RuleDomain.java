package com.luhanlin.spark.warn.domain;

import lombok.Data;

import java.sql.Date;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-04-25 00:47
 */
@Data
public class TZ_RuleDomain {

    private int id;
    private String warn_fieldname; //预警字段
    private String warn_fieldvalue; //预警内容
    private String publisher;      //发布者
    private String send_type;     //消息接收方式
    private String send_mobile;   //接收手机号
    private String send_mail;     //接收邮箱
    private String send_dingding; //接收钉钉
    private Date create_time;    //创建时间
}
