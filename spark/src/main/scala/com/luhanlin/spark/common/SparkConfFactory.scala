package com.luhanlin.spark.common

import java.io.IOException
import java.util.Properties

import org.apache.commons.io.IOUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

import scala.collection.JavaConversions
/**
  * author: KING
  * description:
  * Date:Created in 2019-05-21 21:04
  */
object SparkConfFactory extends Serializable with Logging {
    private val DEFAULT_BATCH_PATH      = "/spark/spark-batch-config.properties"
    private val DEFAULT_STREAMING_PATH  = "/spark/spark-streaming-config.properties"
    private val DEFAULT_STARTWITHJAVA_PATH  = "/spark/spark-start-config.properties"

    /**
    * 获取sparkconf
    * @param appName
    * @param threads
    * @return
    */
    def newSparkLocalConf(appName: String = "spark local", threads: Int = 1): SparkConf = {
        new SparkConf().setMaster(s"local[$threads]").setAppName(appName)
    }

    def newSparkConf(appName:String = "default") : SparkConf = {
        new SparkConf().setAppName(appName)
    }

    def newSparkLocalStreamingContext(appName: String = "sparkStreaming",
                                    batchInterval:Long=30L,
                                    threads:Int=4): StreamingContext ={

        val sparkConf = SparkConfFactory.newSparkLocalConf(appName,threads)
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1")
        new StreamingContext(sparkConf,Seconds(batchInterval))

    }

    /**
      *  SparkStreaming 集群处理 SparkConf
      * @param appName
      * @return
      */
    def newSparkStreamingConf(appName:String = "default") : SparkConf = {
        val sparkConf = newSparkBatchConf(appName)
        sparkConf.setAll(readConfigFileAsTraversable(DEFAULT_STREAMING_PATH))
        sparkConf
    }

    /**
      * sparkCore 离线集群批量处理parkConf
      * @param appName
      * @return
      */
    def newSparkBatchConf(appName:String = "defualt") : SparkConf = {
        val sparkConf = newSparkConf(appName)
        sparkConf.setAll(readConfigFileAsTraversable(DEFAULT_BATCH_PATH))
        sparkConf
    }


    /**
      * spark 配置文件读取
      * Traversable 是scala所有集合的 最顶层 接口
      * @param path
      * @return
      */
    private def readConfigFileAsTraversable(path:String) : Traversable[(String,String)] = {

        val prop = new Properties()
        val source = SparkConfFactory.getClass().getResourceAsStream(path)
        if( source == null ){
            logError( s"未加载到 配置文件  $path 的数据..." )
        }else{
            try{
                prop.load(source);
            }catch{
                case e:IOException =>  logError(s"加载配置文件$path 失败。", e);
            }finally {
                //将流关闭
                IOUtils.closeQuietly(source)
            }
        }
        val values = JavaConversions.collectionAsScalaIterable(prop.entrySet())
        val kvs = values.filter(map=>{map.getValue!=null})
                .map( map => {( map.getKey().toString().trim(), map.getValue().toString().trim())})
                .filter( !_._2.isEmpty() )

        logInfo( s"加载配置文件  $path 成功,具体参数如下：")
        logInfo(kvs.toList.toString)

        kvs

    }



}
