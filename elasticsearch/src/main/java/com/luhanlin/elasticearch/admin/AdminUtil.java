package com.luhanlin.elasticearch.admin;


import com.bigdata.common.file.FileCommon;
import com.luhanlin.elasticearch.client.ESClientUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author:
 * @description:
 * @Date:Created in 2018-04-03 16:12
 */
public class AdminUtil {
    private static Logger LOG = LoggerFactory.getLogger(AdminUtil.class);

    public static void main(String[] args) throws Exception{
        //创建索引核mapping
        AdminUtil.buildIndexAndTypes("tanslator_test1111","tanslator_test1111", "es/mapping/test.json",3,1);
        //index = 类型+日期

    }


    /**
     *
     * @param index
     * @param type
     * @param path
     * @param shard
     * @param replication
     * @return
     * @throws Exception
     */
    public static boolean buildIndexAndTypes(String index,String type,String path,int shard,int replication) throws Exception{
        boolean flag ;
        TransportClient client = ESClientUtils.getClient();
        String mappingJson = FileCommon.getAbstractPath(path);

        boolean indices = AdminUtil.createIndices(client, index, shard, replication);
        if(indices){
            LOG.info("创建索引"+ index + "成功");
            flag = MappingUtil.addMapping(client, index, type, mappingJson);
        }
        else{
            LOG.error("创建索引"+ index + "失败");
            flag = false;
        }
        return flag;
    }



    /**
     * @desc 判断需要创建的index是否存在
     * */
    public static boolean indexExists(TransportClient client,String index){
        boolean ifExists = false;
        try {
            System.out.println("client===" + client);
            IndicesExistsResponse existsResponse = client.admin().indices().prepareExists(index).execute().actionGet();
            ifExists = existsResponse.isExists();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("判断index是否存在失败...");
            return ifExists;
        }
        return ifExists;
    }


    /**
     * 创建索引
     * @param client
     * @param index
     * @param shard
     * @param replication
     * @return
     */
    public static boolean createIndices(TransportClient client, String index, int shard , int replication){

        if(!indexExists(client,index)) {
            LOG.info("该index不存在，创建...");
            CreateIndexResponse createIndexResponse =null;
            try {
                createIndexResponse = client.admin().indices().prepareCreate(index)
                        .setSettings(Settings.builder()
                                .put("index.number_of_shards", shard)
                                .put("index.number_of_replicas", replication)
                                .put("index.codec", "best_compression")
                                .put("refresh_interval", "30s"))
                        .execute().actionGet();
                return createIndexResponse.isAcknowledged();
            } catch (Exception e) {
                LOG.error(null, e);
                return false;
            }
        }
        LOG.warn("该index " + index + " 已经存在...");
        return false;
    }



}
