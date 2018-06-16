package evi_thesis.flink.clustering.visualization

import java.net.{InetAddress, InetSocketAddress}
import java.util

import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.flink.api.scala._
import java.util.Date

//read files produced by OnlineKMeansJob and send them to Kibana

object ClusterVisualization extends java.io.Serializable{

  def main(args: Array[String]): Unit =
  {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

     /**********visualize centroids**************/
    val clusters = env.readTextFile("./Results/NewCentroids.txt").setParallelism(1).map( line =>
    {
      val timeFormatter : DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")
      val token = line.substring(12).filterNot(c=>c=='(' || c==')').split(",")
      new Tuple5(token(0).toDouble, token(1).toDouble, token(2).toInt, new DateTime(token(3).toLong), token(4).toInt)
    })

    //If you want to visualize the clustered points, remove the comments and put the "visualize centroid" section in comments
    /********visualize clustered points**************/
   // val clusters = env.readTextFile("./Results/Partition.txt").map( line =>
   // {
   //   val timeFormatter : DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")
   //   val token = line.filterNot(c=>c=='(' || c==')').split(",")
  //    new Tuple5(token(0).toDouble, token(1).toDouble, token(2).toInt, new DateTime(token(3).toLong), token(4).toInt) //*1000 in data source
  //  })

    /**sent data to Elasticsearch Sink**/
    val config = Map(
      // This instructs the sink to emit after every element, otherwise they would be buffered
      "bulk.flush.max.actions" -> "10",
      // default cluster name
      "cluster.name" -> "elasticsearch"
    )
    val jConfig: java.util.Map[String, String] = new java.util.HashMap()
    jConfig.putAll(config.asJava)

    //configure connection with ElasticSearch Sink
    val transports = List(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))
    val jTransports = new util.ArrayList(transports.asJava)
    clusters.addSink(new ElasticsearchSink(jConfig, jTransports, new StartPlacesInserter))



 env.execute()


  }
}
