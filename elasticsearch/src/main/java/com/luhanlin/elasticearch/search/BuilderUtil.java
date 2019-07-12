package com.luhanlin.elasticearch.search;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuilderUtil {

    private static Logger LOG = LoggerFactory.getLogger(BuilderUtil.class);

    public static SearchRequestBuilder getSearchBuilder(TransportClient client, String index, String type){
        SearchRequestBuilder builder = null;
        try {
            if (StringUtils.isNotBlank(index)) {
                builder = client.prepareSearch(index.split(","));
            } else {
                builder = client.prepareSearch();
            }
            if (StringUtils.isNotBlank(type)) {
                builder.setTypes(type.split(","));
            }
        } catch (Exception e) {
            LOG.error(null, e);
        }
        return builder;
    }

    public static SearchRequestBuilder getSearchBuilder(TransportClient client, String[] indexs, String type){
        SearchRequestBuilder builder = null;
        try {
            if (indexs.length>0) {
                for(String index:indexs){
                    builder = client.prepareSearch(index);
                }
            } else {
                builder = client.prepareSearch();
            }
            if (StringUtils.isNotBlank(type)) {
                builder.setTypes(type);
            }
        } catch (Exception e) {
            LOG.error(null, e);
        }
        return builder;
    }


}
