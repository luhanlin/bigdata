package com.bigdata.firstdemo.yarn.telflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/24 23:40]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class FlewDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2. 获取jar
        job.setJarByClass(FlewDriver.class);

        // 3. 设定mapper和reducer
        job.setMapperClass(FlewMapper.class);
        job.setReducerClass(FlewReducer.class);

        // 3. 设定map输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlewWritable.class);

        // 4. 设定reduce输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 5. 设定读取文件路径和输出结果文件路径
        FileInputFormat.setInputPaths(job,new Path("E:/big_data/mapreducetest/flew/input"));
        FileOutputFormat.setOutputPath(job, new Path("E:/big_data/mapreducetest/flew/output"));

        job.waitForCompletion(true);
    }
}
