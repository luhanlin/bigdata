package com.bigdata.firstdemo.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/24 00:13]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class WordCountJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance();

        // 2. 获取jar
        job.setJarByClass(WordCountJob.class);

        // 3. 设定mapper和reducer
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        // 3. 设定map输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4. 设定reduce输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5. 设定读取文件路径和输出结果文件路径
        FileInputFormat.setInputPaths(job,new Path("E:\\big_data\\mapreducetest\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\big_data\\mapreducetest\\output"));

        boolean res = job.waitForCompletion(true);

        System.out.println(res ? "success" : false);

    }
}
