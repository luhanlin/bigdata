package com.bigdata.firstdemo.rpc.protocol;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/22 08:37]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public interface MyNameNodeProtocol {

    long versionID = 1L;

    public String getMataMsg(String path);
}
