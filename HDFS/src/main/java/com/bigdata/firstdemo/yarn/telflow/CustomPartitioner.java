package com.bigdata.firstdemo.yarn.telflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/2/23 15:20]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class CustomPartitioner extends Partitioner<Text, FlewWritable> {

    @Override
    public int getPartition(Text text, FlewWritable flewWritable, int i) {
        // 1. 截取手机号码前三位
        String phoneThirdNum = text.toString().substring(0, 3);

        // 2. 定义分区
        int partition = 4;

        if ("135".equals(phoneThirdNum)){
            return 0;
        }
        if ("138".equals(phoneThirdNum)){
            return 1;
        }
        if ("133".equals(phoneThirdNum)){
            return 2;
        }
        if ("158".equals(phoneThirdNum)){
            return 3;
        }

        return partition;
    }
}
