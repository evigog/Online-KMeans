package evi_thesis.flink.clustering

import java.util.concurrent.TimeUnit

import evi_thesis.flink.clustering.timestamps.ModelTimestamps
import evi_thesis.flink.clustering.update_cluster.{WindowCentroid, ReduceCentroids}

import evi_thesis.flink.datatypes.{DataVector, Centroid, OnlineKMeansModel}
//import org.apache.calcite.avatica.util.TimeUnit
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, GlobalWindows}

import scala.collection.mutable.ListBuffer

class StepFunction(trainStream: DataStream[DataVector[Double]]) extends (DataStream[OnlineKMeansModel] => (DataStream[OnlineKMeansModel], DataStream[OnlineKMeansModel]))
{

  def apply(model: DataStream[OnlineKMeansModel]):
  (DataStream[OnlineKMeansModel], DataStream[OnlineKMeansModel]) =
  {

    //calculate eucleidian distance from point to every centroid- i need both streams for that
    //determine label (cluster id) for every point of trainStream
    val con1 =model.connect(trainStream)

    val distance = con1.flatMap(new SelectNearestCenter()).map(x => new Tuple3(x._1, x._2, 1.0)) //distance->(Cluster_Centroid, DataVector, 1)

    //partition points by cluster id
    val partition = distance.keyBy(in => in._1.cluster) //keyBy clusterID
    val visualization = partition.map(t=>(t._2.data(0), t._2.data(1), t._1.cluster, t._2.timestamp, t._1.round))
    visualization.writeAsText("./Results/Partition.txt") //right format for visualisation


    //split new cluster points into tumbling windows - windows are different for each key
    val keyedWindows = partition.timeWindow(Time.of(25, TimeUnit.MINUTES))

    //compute overall centroids: compute centroid of current keyedWindow and merge it with the last one
    val newCentroids: DataStream[(Centroid,Centroid)] = keyedWindows.apply(
      new ReduceCentroids(), new WindowCentroid() )
    newCentroids.map(t=>(t._2.center.data, t._2.cluster, t._2.center.timestamp, t._2.round)).writeAsText("./Results/NewCentroids.txt") //newCentroids->(lastCentroid, newCentroid)


    val LocalModel = newCentroids.map(c => new OnlineKMeansModel(ListBuffer(c._2), c._2.round)) //merge centroids from current slot in one local model

    //Merging Local Models
    val OverallModel:DataStream[OnlineKMeansModel] = LocalModel
     .windowAll(TumblingEventTimeWindows.of(Time.of(2, TimeUnit.MILLISECONDS))) //each window contains all models from the same round
      .apply(new ReduceModels(), new MergeModels()) // merge models


      OverallModel.setParallelism(4).writeAsText("./Results/OverallModel.txt")

    //broadcast local models for feedback and output
    (OverallModel.broadcast, OverallModel.broadcast)
  }
}


