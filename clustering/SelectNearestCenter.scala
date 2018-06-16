package evi_thesis.flink.clustering

import breeze.linalg.norm
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer



import evi_thesis.flink.datatypes._


/** Determines the closest cluster center for a data point. **/
class SelectNearestCenter extends CoFlatMapFunction[OnlineKMeansModel, DataVector[Double], (Centroid, DataVector[Double])]
{
  private var model: OnlineKMeansModel = null

  //add buffer to store elements that came before new model
  private var buffPoints = new ArrayBuffer[DataVector[Double]](100)

  //gets data from the modelStream
  override def flatMap1(m: OnlineKMeansModel, out1: Collector[(Centroid, DataVector[Double])]): Unit =
  {

    if(Option(model).isDefined) //compare with the last model
    {
      val oldCent = model.centroids
      val newCent = m.centroids
      oldCent.foreach( o=>
      {
        if (!newCent.exists(n=>n.cluster==o.cluster))
          newCent.append(o)
      })

      model = new OnlineKMeansModel(newCent, m.round) //update the stored state
    }
    else
      model = m

  }

  //gets data from the trainingStream
  override def flatMap2(p: DataVector[Double], out2: Collector[(Centroid, DataVector[Double])]): Unit =
  {

    if (Option(model).isDefined)
    {
      //check buffer first
      if (buffPoints.nonEmpty)
      {
        buffPoints.map(b=>assignToCluster(b)).foreach(r=>out2.collect(r)) //assign each point of the buffer to the clostest cluster
        buffPoints = new ArrayBuffer[DataVector[Double]] //buffer is empty now
      }

      out2.collect(assignToCluster(p)) //assign new point to cluster
    }
    else
    {
      //store elements in buffer - wait for model
      buffPoints.append(p)
    }

  }

  def assignToCluster(p:DataVector[Double]):(Centroid, DataVector[Double]) =
  {
    var minDistance = Long.MaxValue.toDouble
    var closestCentroid : Centroid = null

    for (c <- model.centroids) //check all cluster centers
    {
      val d = norm(c.center.data - p.data) //compute euclidean distance from point to each centroid
      // update nearest cluster if necessary
      if (d <= minDistance)
      {
        minDistance = d
        closestCentroid = c //keep clusterID
      }
    }
    (closestCentroid, p) //(clusterID, point)
  }

}
