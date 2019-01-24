package com.bigdata.firstdemo.yarn.telflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/24 23:29]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class FlewReducer extends Reducer<Text, FlewWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<FlewWritable> values, Context context) throws IOException, InterruptedException {

        //1.  将相同手机号的流量再次汇总
        long flewInSum = 0L;
        long flewOutSum = 0L;

        for (FlewWritable flew: values) {
            flewInSum = flew.getFlewIn();
            flewOutSum = flew.getFlewOut();
        }

        context.write(key, new Text(new FlewWritable(flewInSum, flewOutSum).toString()));
    }
}
