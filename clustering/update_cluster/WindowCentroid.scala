package evi_thesis.flink.clustering.update_cluster

import evi_thesis.flink.datatypes._

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import java.util.Date

//compute the new centroid as the average of the new points assigned to this cluster
//merge the new centroid with the old one for this cluster

class WindowCentroid() extends RichWindowFunction[(Centroid,DataVector[Double],Double), (Centroid, Centroid), Int, TimeWindow]
{
  //keep last computed centroid as a state - window state is different per key(per cluster)
  var lastCentroid:ValueStateDescriptor[Centroid] = new ValueStateDescriptor[Centroid]("last-centroid", new TypeHint[Centroid](){}.getTypeInfo,null)

  override def apply(key: Int, window: TimeWindow, iterable: Iterable[(Centroid, DataVector[Double], Double)], out: Collector[(Centroid, Centroid)] ) =
  {
    //access decay factor
    val a = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap.get("job.decayfactor").toDouble

    val p: (Centroid, DataVector[Double], Double) = iterable.toIterator.next() //get input value, produced by ReduceCentroids
    val oldCent = getRuntimeContext.getState(lastCentroid) //get WindowState
    var cur_round:Int=0
    if (oldCent.value()!=null) cur_round=oldCent.value().round

    //calculate average - centroid from current window
    val newCent = new Centroid( DataVector( p._2.data :/= p._3, p._2.timestamp), p._1.cluster,  p._3, cur_round+1)

    //merge newCent and oldCent
    var mergeCent:Centroid = null
    var last:Centroid = null
    if(oldCent.value()==null)
    {
      mergeCent=newCent
      last = newCent
    }
    else
    {
      val size = newCent.size + oldCent.value().size //old size plus new
      val b = oldCent.value().size * a + newCent.size
      val c1 = oldCent.value().size * a / b
      val c2 = newCent.size / b
      val c = c1 * oldCent.value().center.data  + c2 * newCent.center.data
      mergeCent = new Centroid(DataVector[Double](c, p._2.timestamp), newCent.cluster, size, newCent.round)
      last = oldCent.value()
    }

    oldCent.update(mergeCent) //update WindowState with the latest centroid
    out.collect((last, mergeCent)) //return last centroid just for debugging

  }

}
