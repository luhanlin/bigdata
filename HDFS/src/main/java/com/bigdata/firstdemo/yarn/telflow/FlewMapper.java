package com.bigdata.firstdemo.yarn.telflow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/24 23:18]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class FlewMapper extends Mapper<LongWritable,Text,Text,FlewWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 读取数据
        String line = value.toString();

        //3631279850362	13726130503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	www.itstaredu.com	教育网站	24	27	299	681	200
        // 13726130503 299	681
        // 2. 一行数据拆分
        System.out.println("===========" + line);
        String[] split = line.split("\t");

        String tel = split[1];
        FlewWritable flewWritable = new FlewWritable(Long.parseLong(split[split.length - 3]), Long.parseLong(split[split.length - 2]));

        // 3. 循环的写到下一个阶段reduce中
        context.write(new Text(tel), flewWritable);

    }
}
