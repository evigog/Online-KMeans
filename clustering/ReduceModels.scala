package evi_thesis.flink.clustering

import evi_thesis.flink.datatypes.{OnlineKMeansModel, DataVector, Centroid}
import org.apache.flink.api.common.functions.ReduceFunction

//merge centroids from the same round in the same buffer
class ReduceModels extends ReduceFunction[OnlineKMeansModel] {

  override def reduce(m1:OnlineKMeansModel, m2:OnlineKMeansModel):OnlineKMeansModel =
  {
   new OnlineKMeansModel(m1.centroids++=m2.centroids, m1.round)
  }

}
