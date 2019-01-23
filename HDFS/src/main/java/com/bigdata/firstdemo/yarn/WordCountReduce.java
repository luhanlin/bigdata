package com.bigdata.firstdemo.yarn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/24 00:08]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1. 定义单词出现的次数
        int count = 0;

        // 2. 循环计数
        for (IntWritable value: values) {
            count += value.get();
        }

        // 3. 输出结果
        context.write(key, new IntWritable(count));
    }
}
