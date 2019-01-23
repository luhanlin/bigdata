package com.bigdata.firstdemo.rpc.server;

import com.bigdata.firstdemo.rpc.protocol.MyNameNodeProtocol;
import com.bigdata.firstdemo.rpc.protocol.MyNameNodeProtocolImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * <类详细描述> rpc server
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/22 08:24]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class PublishServer {

    public static void main(String[] args) throws IOException {
        // 1. 构建RPC框架
        RPC.Builder builder = new RPC.Builder(new Configuration());

        // 2. 绑定地址和端口
        RPC.Server server = builder.setBindAddress("localhost")
                .setPort(7777)
        // 3. 绑定协议
                .setProtocol(MyNameNodeProtocol.class)
                .setInstance(new MyNameNodeProtocolImpl())
                .build();

        // 4. start
        server.start();
    }


}
