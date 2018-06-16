package evi_thesis.flink.clustering.update_cluster

import evi_thesis.flink.datatypes._
import org.apache.flink.api.common.functions.ReduceFunction

import breeze.linalg.DenseVector


//add all the points of current keyedWindow assigned to this cluster

class ReduceCentroids extends ReduceFunction[(Centroid, DataVector[Double], Double)]
{

  override def reduce(p1: (Centroid, DataVector[Double], Double), p2: (Centroid, DataVector[Double], Double)):(Centroid, DataVector[Double], Double) =
  {
    val sum:DenseVector[Double] = p1._2.data + p2._2.data

    //find the max timestamp of the input elements
    val maxtimestamp = {if (p1._2.timestamp > p2._2.timestamp) p1._2.timestamp else p2._2.timestamp}

    //(lastCentroid, DataVector, sizeOfCluster)
    (p1._1, DataVector(sum, maxtimestamp ), p1._3 + p2._3)
  }

}
