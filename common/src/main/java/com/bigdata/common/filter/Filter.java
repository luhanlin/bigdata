package com.bigdata.common.filter;

/**
 * Description:
 *      数据过滤顶层接口
 * @author
 * @version 1.0
 * @date 2017/7/10 15:21
 */
@FunctionalInterface
public interface Filter<T> {
    boolean filter(T obj);
}
