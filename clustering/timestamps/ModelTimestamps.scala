package evi_thesis.flink.clustering.timestamps

import evi_thesis.flink.datatypes._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class ModelTimestamps extends AssignerWithPeriodicWatermarks[OnlineKMeansModel]
{

  var counter = 0L

  override def extractTimestamp(element:OnlineKMeansModel, previousElementTimestamp : Long) : Long =  {
    counter = element.round
    val t= 10*(element.round)+element.centroids.head.cluster //
    t
  }

  override def getCurrentWatermark(): Watermark = new Watermark(10*counter)


}
