package com.bigdata.firstdemo.rpc.client;

import com.bigdata.firstdemo.rpc.protocol.MyNameNodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * <类详细描述> client
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/22 21:50]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class MyNameNodeClient {

    public static void main(String[] args) throws IOException {
        // 1. 拿到协议
        MyNameNodeProtocol proxy = RPC.getProxy(MyNameNodeProtocol.class, 1L,
                new InetSocketAddress("localhost", 7777), new Configuration());

        // 2. 获取元数据
        String mataMsg = proxy.getMataMsg("allen");

        System.out.println(mataMsg);
    }

}
