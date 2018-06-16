package evi_thesis.flink.clustering

import evi_thesis.flink.datatypes.OnlineKMeansModel
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//send model to output
class MergeModels extends AllWindowFunction[OnlineKMeansModel, OnlineKMeansModel, TimeWindow]
{

  override def apply(window:TimeWindow, iterable:Iterable[OnlineKMeansModel], out:Collector[OnlineKMeansModel]) =
  {

    val m = iterable.toIterator.next()

    val windowmaxtmstp = window.maxTimestamp() //end of window

    out.collect(m)

  }

}

