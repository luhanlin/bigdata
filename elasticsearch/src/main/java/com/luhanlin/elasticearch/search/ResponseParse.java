package com.luhanlin.elasticearch.search;

import org.elasticsearch.action.get.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author: chenhailong
 * @description:
 * @Date:Created in 2018-04-09 16:55
 */
public class ResponseParse {

    private static Logger LOG = LoggerFactory.getLogger(BuilderUtil.class);

    public static Map<String, Object> parseGetResponse(GetResponse getResponse){
        Map<String, Object> source = null;
        try {
            source = getResponse.getSource();
        } catch (Exception e) {
            LOG.error(null,e);
        }
        return source;
    }

}
