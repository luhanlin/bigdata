package com.luhanlin.spark.streaming.kafka

import org.apache.spark.Logging

/**
  * author:
  * description:
  * Date:Created in 2019-04-21 22:20
  */
object Spark_Es_ConfigUtil extends Serializable with Logging{

  val ES_NODES = "es.nodes"
  val ES_PORT = "es.port"
  val ES_CLUSTERNAME = "es.cluster.name"


  def getEsParam(id_field : String): Map[String,String] ={
    Map[String ,String]("es.mapping.id" -> id_field,
      ES_NODES -> "bigdata121",
      ES_PORT -> "9200",
      ES_CLUSTERNAME -> "test",
      "es.batch.size.entries"->"6000",
      /*   "es.nodes.wan.only"->"true",*/
      "es.nodes.discovery"->"true",
      "es.batch.size.bytes"->"300000000",
      "es.batch.write.refresh"->"false"
    )
  }
}
