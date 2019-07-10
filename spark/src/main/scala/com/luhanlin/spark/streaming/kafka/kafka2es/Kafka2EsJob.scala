package com.luhanlin.spark.streaming.kafka.kafka2es

import com.luhanlin.spark.common.convert.DataConvert
import com.luhanlin.spark.streaming.kafka.Spark_Es_ConfigUtil
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark.rdd.EsSpark

/**
  * author: KING
  * description:
  * Date:Created in 2019-05-23 21:44
  */
object Kafka2EsJob extends Serializable with Logging{


  /**
    * 将RDD转换之后写入ES
    * @param dataType
    * @param typeDS
    */
    def insertData2Es(dataType:String,typeDS:DStream[java.util.Map[String,String]]): Unit ={

      val index = dataType
      typeDS.foreachRDD(rdd=>{
        val esRDD =  rdd.map(x=>{
          DataConvert.strMap2esObjectMap(x)
        })
        EsSpark.saveToEs(esRDD,index+ "/"+index,Spark_Es_ConfigUtil.getEsParam("id"))
        println("写入ES" + esRDD.count() + "条数据成功")
      })
    }

}
