package evi_thesis.flink.datatypes

import scala.collection.mutable.ListBuffer

case class OnlineKMeansModel(centroids: ListBuffer[Centroid], round:Int) //round: round of computation

