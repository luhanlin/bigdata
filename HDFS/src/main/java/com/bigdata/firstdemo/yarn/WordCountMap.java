package com.bigdata.firstdemo.yarn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/24 00:03]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class WordCountMap extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 读取数据
        String line = value.toString();

        // 2. 一行数据拆分
        String[] split = line.split(" ");

        // 3. 循环的写到下一个阶段reduce中
        for (String s : split) {
            context.write(new Text(s.getBytes("utf-8")), new IntWritable(1));
        }

    }
}
