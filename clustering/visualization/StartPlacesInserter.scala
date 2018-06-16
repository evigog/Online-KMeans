package evi_thesis.flink.clustering.visualization

import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.apache.flink.streaming.connectors.elasticsearch2._
import org.apache.flink.api.common.functions.RuntimeContext
import scala.collection.JavaConverters._
import java.util.Date
import org.joda.time.DateTime

//create Elasticsearch index

class StartPlacesInserter extends ElasticsearchSinkFunction[Tuple5[Double,Double,Int,DateTime,Int]]
{
  def process(record: Tuple5[Double,Double,Int,DateTime,Int], ctx: RuntimeContext, indexer: RequestIndexer)
  {
    val json = Map(
        "location" -> (record._2.toString + "," + (-record._1).toString),
        "clusterID" -> record._3.toString,
        "timestamp" -> record._4.toString("yyyy-MM-dd HH:mm:ss"),
        "round" -> record._5.toString
      )

      val rqst: IndexRequest = Requests.indexRequest
        .index("porto-taxi")
        .`type`("start-points")
        .source(json.asJava)

      indexer.add(rqst)

  }
}
