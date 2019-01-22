package com.bigdata.firstdemo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * <类详细描述> hdfs api test
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/21 23:23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class HdfsApiTest {

    FileSystem fs = null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        // 1. 加载配置项
        Configuration conf = new Configuration();

        // 2. 设置副本数
        conf.set("dfs.replication", "2");

        // 3. 设置块大小
        conf.set("dfs.blocksize", "64m");

        // 3. 构建客户端
        fs = FileSystem.get(new URI("hdfs://192.168.134.121:9000"), conf, "root");
    }

    @Test
    public void test01() throws IOException {
        // 添加folder
        fs.mkdirs(new Path("/test001"));
        fs.close();
    }

    @Test
    public void test02() throws IOException {
        // delete folder
        fs.delete(new Path("/test001"), true);
        fs.close();
    }

    @Test
    public void test03() throws IOException {
        // list files
        RemoteIterator<LocatedFileStatus> status = fs.listFiles(new Path("/test01"), true);

        while (status.hasNext()){
            LocatedFileStatus sta = status.next();
            System.out.println(sta.toString());
        }

        fs.close();
    }


}
