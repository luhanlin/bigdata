package com.luhanlin.spark.warn.domain;

import lombok.Data;

import java.sql.Date;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-04-25 05:25
 */
@Data
public class WarningMessage {
    private String id;   //主键id
    private String alarmRuleid;   //规则id
    private String alarmType;     //告警类型
    private String sendType;      //发送方式
    private String sendMobile;    //发送至手机
    private String sendEmail;     //发送至邮箱
    private String sendStatus;    //发送状态
    private String senfInfo;      //发送内容
    private Date hitTime;         //命中时间
    private Date checkinTime;     //入库时间
    private String isRead;        //是否已读
    private String readAccounts;  //已读用户
    private String alarmaccounts;
    private String accountid;

    @Override
    public String toString() {
        return "WarningMessage{" +
                "id='" + id + '\'' +
                ", alarmRuleid='" + alarmRuleid + '\'' +
                ", alarmType='" + alarmType + '\'' +
                ", sendType='" + sendType + '\'' +
                ", sendMobile='" + sendMobile + '\'' +
                ", sendEmail='" + sendEmail + '\'' +
                ", sendStatus='" + sendStatus + '\'' +
                ", senfInfo='" + senfInfo + '\'' +
                ", hitTime=" + hitTime +
                ", checkinTime=" + checkinTime +
                ", isRead='" + isRead + '\'' +
                ", readAccounts='" + readAccounts + '\'' +
                ", alarmaccounts='" + alarmaccounts + '\'' +
                ", accountid='" + accountid + '\'' +
                '}';
    }
}
