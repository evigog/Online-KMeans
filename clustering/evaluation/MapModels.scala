package evi_thesis.flink.clustering.evaluation

import evi_thesis.flink.datatypes._

import org.apache.flink.api.common.functions.RichMapFunction
import scala.collection.mutable.ListBuffer
import breeze.linalg._


//Complete the model if centroids are missing and compute the hypercentoroid

class MapModels extends RichMapFunction[OnlineKMeansModel, (Int, ListBuffer[(Int,Double)])]
{
  private var model:OnlineKMeansModel = null

  override def map(newModel:OnlineKMeansModel):(Int, ListBuffer[(Int,Double)]) =
  {
    //Check if some centroids remain the same
    if (Option(model).isDefined)
    {
      if(newModel.round>model.round) //compare 2 models, if a centroid is missing (cluster didn't change) keep the same one
      {
        val oldCent = model.centroids
        val newCent = newModel.centroids
        oldCent.foreach(o => {
          if (!newCent.exists(n => n.cluster == o.cluster)) //check if any centroid is missing
            newCent.append(o)
        })

        model = new OnlineKMeansModel(newCent, newModel.round) //that's the new model
      }
    }
    else
      model = newModel

    //Compute hypercentroid = sum(cluster_weight *centroid)/sum(cluster_weight)
    //cluster_weights(i) equals with the size of cluster(i)
    val cluster_weights:DenseVector[Double]  = model.centroids.map(c => c.size.toDouble :* c.center.data).reduce((v1,v2)=>v1+v2)
    val mass_sum = model.centroids.map(c=>c.size).sum.toDouble //overall number of points in clusters
    val overall_centroid:DenseVector[Double] = (1/mass_sum) * cluster_weights

    val numOfClusters = model.centroids.length.toDouble

    //compute distance from each centroid to hypercentroid
    val distance = model.centroids.map(c=>(c.cluster, norm((overall_centroid-c.center.data))/numOfClusters) )

    (newModel.round, distance)


  }

}
