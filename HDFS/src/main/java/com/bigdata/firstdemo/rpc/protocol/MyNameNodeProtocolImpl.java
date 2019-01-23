package com.bigdata.firstdemo.rpc.protocol;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/22 08:45]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class MyNameNodeProtocolImpl implements MyNameNodeProtocol{
    @Override
    public String getMataMsg(String path) {
        return path + " hello world";
    }
}
