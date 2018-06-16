package evi_thesis.flink.clustering

import breeze.linalg.DenseVector
import evi_thesis.flink.clustering.visualization.StartPlacesInserter
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import evi_thesis.flink.StreamingJob
import evi_thesis.flink.clustering.training.TrainingVectorSource
import evi_thesis.flink.clustering.timestamps.ModelTimestamps
import evi_thesis.flink.datatypes._
import evi_thesis.flink.clustering.evaluation._


//debugging: java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -cp /home/evigog/evi-thesis/flink-online-kmeans/scala/target/flink-online-kmeans-0.0.1-fat.jar evi_thesis.flink.clustering.OnlineKMeansJob


object OnlineKMeansJob extends StreamingJob {

  override def run(env: StreamExecutionEnvironment): Unit =
  {
    /******** model stream ********/
    /***initialization #1***/
    //define a grid in the porto area (training file) and take the four corners
    //val c1 = Array(7.743915, 41.008275) //(min, min)
    //val c2 = Array(7.743915, 41.296464) //(min, max)
    //val c3 = Array(8.698356, 41.008275) //(max, min)
    //val c4 = Array(8.698356, 41.296464) //(max, max)

    /***initialization #2***/
    //define a grid in the porto area (metadata file) and take the four corners
   // val c1 = Array(8.5681953968, 41.1405168841) //(min, min)
   /// val c2 = Array(8.5681953968, 41.1835097223) //(min, max)
  //  val c3 = Array(8.6891799603, 41.1405168841) //(max, min)
  //  val c4 = Array(8.6891799603, 41.1835097223) //(max, max)\

    /***initialization #3***/
    //split the grid above in 4 and take the four centers
    val c1 = Array(8.5984415377, 41.15126509365)
    val c2 = Array(8.5984415377, 41.17276151275)
    val c3 = Array(8.65893381945, 41.15126509365)
    val c4 = Array(8.65893381945, 41.17276151275)


    val init = ListBuffer[Centroid] (
      Centroid(new DataVector(new DenseVector[Double](c1), -1), 1, 1, 0),
      Centroid(new DataVector(new DenseVector[Double](c2), -1), 2, 1, 0),
      Centroid(new DataVector(new DenseVector[Double](c3), -1), 3, 1, 0),
      Centroid(new DataVector(new DenseVector[Double](c4), -1), 4, 1, 0)
    )

     //initialize modelStream with init centroids
    val modelStream = env.fromElements(0).map(e => OnlineKMeansModel(init, 0))

    /** *****training data stream *******/
    //load training set
    val trainingSource = env.addSource(new TrainingVectorSource()).shuffle
    trainingSource.writeAsText("./Results/TrainDataStream.txt")


    /***Training***/
   val trainedModelStream = modelStream.iterate (new StepFunction(trainingSource)).setParallelism(1)
   trainedModelStream.map(m=>(m.round, m.centroids.map(c=>(c.cluster, c.size)))).writeAsText("./Results/TrainedModelStream.txt")


    /***Clustering Evaluation***/
    //1. average distance from global centroid
   val evaluate = trainedModelStream.shuffle.map(new MapModels())
   evaluate.writeAsText("./Results/EvaluateClustering.txt")

    //2. max_distance/min_distance
   val maxmin_distance = evaluate.shuffle.map(new MaxMin_Distance())
   maxmin_distance.writeAsText("./Results/MaxMin.txt")

    env.execute()
  }
}

