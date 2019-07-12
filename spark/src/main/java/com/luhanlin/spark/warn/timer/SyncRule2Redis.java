package com.luhanlin.spark.warn.timer;

import java.util.TimerTask;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-05-30 20:56
 */
public class SyncRule2Redis extends TimerTask {
    @Override
    public void run() {
        //这里定义同步方法
        //就是读取mysql的数据 然后写入到redis中
        System.out.println("========开始同步MYSQL规则到redis=======");
        WarnHelper.syncRuleFromMysql2Redis();
        System.out.println("============开始同步规则成功===========");
    }
}
