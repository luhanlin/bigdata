package com.bigdata.firstdemo.wordcount;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/22 22:29]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class WordCountMain {

    public static Map<String, Integer> wordCountMap = new HashMap();

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        String input_path = "/test01/";
        String output_res = "/result/res.txt";
        System.out.println("input_path = " + input_path + "\noutput_res = " + output_res);

        // 1. 得到hdfs文件
        Configuration conf = new Configuration();

        // 2. 获取客户端
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.134.121:9000"), conf, "root");

        // 3. 读取文件
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(input_path),true);

        while (iterator.hasNext()){
            LocatedFileStatus next = iterator.next();
            Path path = next.getPath();

            FSDataInputStream in = fs.open(path);

            BufferedReader read = new BufferedReader(new InputStreamReader(in,"UTF-8"));

            String line = null;

            while ((line = read.readLine()) != null){

                String[] split = line.split(" ");
                for (String s: split) {
                    if (wordCountMap.containsKey(s)){
                        wordCountMap.put(s, wordCountMap.get(s) + 1);
                    } else {
                        wordCountMap.put(s, 1);
                    }
                }
            }
        }

        // 将结果存到指定目录下
        Path path = new Path(output_res);
        if (!fs.exists(path)){
            fs.mkdirs(new Path("/result"));
        }

        FSDataOutputStream out = fs.create(path);

        Set<Map.Entry<String, Integer>> entries = wordCountMap.entrySet();

        for (Map.Entry<String, Integer> entry: entries) {
            out.write((entry.getKey() + "\t" + entry.getValue() + "\n").getBytes());
        }

        out.close();
        fs.close();

        System.out.println("word count analyze successfully");
    }

}
