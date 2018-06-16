package evi_thesis.flink.clustering.evaluation

import org.apache.flink.api.common.functions.MapFunction
import breeze.linalg.norm

import scala.collection.mutable.ListBuffer

//comput the ratio of maximun disatance from hypercentroid to minimum disatance from hypercentroid
class MaxMin_Distance extends MapFunction[(Int, ListBuffer[(Int,Double)]), (Int,Double)]
  {

  override def map(list:(Int, ListBuffer[(Int,Double)])): (Int,Double) =
  {
    val distance = list._2.map(d=>d._2)

    val max_min = distance.max/distance.min
    (list._1, max_min)

  }

}
