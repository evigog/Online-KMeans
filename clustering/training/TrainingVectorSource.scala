package evi_thesis.flink.clustering.training

import breeze.linalg.DenseVector
import org.apache.flink.streaming.api.functions.source.SourceFunction
import evi_thesis.flink.datatypes.DataVector
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.DateTime
import java.util.Calendar
import java.util.Random;

class TrainingVectorSource extends SourceFunction[DataVector[Double]]
{
  var timestamp=0
  val groupNumber = 10

  val servingSpeed = 1200 //fast-forward the stream, events happened in 2 minutes are served in one second
  val maxDelayMsecs = 120000 //maximum serving delay which causes each event to be randomly delayed within the specified bound
  val watermarkDelayMSecs = { if(maxDelayMsecs < 10000) 10000 else maxDelayMsecs }
  var rand = new Random(7452)

  override def run(ctx:SourceContext[DataVector[Double]]):Unit =
  {
    val bufferedSource = scala.io.Source.fromFile("./week.csv") //input dataset (data must be sorted by ascending timestamp)
                         .getLines()
                           .filterNot(l=>l.containsSlice("[]") | l.contains("TRIP_ID")) //delete lines with missing values and headers
    for (line <- bufferedSource)
    {
      val token = line.split(",")
      val startLon = token(8).filterNot(c=>c=='-' | c=='[' | c=='\"').toDouble //latitude of starting position
      val startLat = token(9).filterNot(c=> c==']' | c=='\"').toDouble //longitude of starting position
      val tmstmp = token(5).filterNot(c=>c=='\"').toLong * 1000 // timestamp of the journey start (in milliseconds)

      /***accelerate input elements***/
        val now = Calendar.getInstance().getTimeInMillis()
        val servingStartTime = Calendar.getInstance().getTimeInMillis()
        val dataStartTime = tmstmp
        val delayedEventTime = dataStartTime + getNormalDelayMsecs(rand) //add a random delay to each element
        val servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime)
        val waitTime = servingTime - now
        Thread.sleep({if (waitTime > 0)  waitTime else 0})

      val v =  new DataVector(new DenseVector( Array(startLon, startLat)), tmstmp) //each new element is represented by a Vector

      timestamp = timestamp + 1
      ctx.collectWithTimestamp(v, tmstmp) //create Timestamp for Watermarks - event timestamps(in milliseconds) used

      //emit Watermarks
      if (timestamp % groupNumber == 0) { //emit watermarks every 10 elements
        val watermarkTime = delayedEventTime + watermarkDelayMSecs
        ctx.emitWatermark(new Watermark(watermarkTime - maxDelayMsecs - 1))
      }
    }

  }

  override def cancel(): Unit = {
    // No cleanup needed
  }

  def toServingTime(servingStartTime:Long, dataStartTime:Long, eventTime:Long):Long =
  {
    val dataDiff = eventTime - dataStartTime
    return servingStartTime + (dataDiff / servingSpeed)
  }

  def getNormalDelayMsecs(rand:Random):Long =
  {
    var delay = -1L
    val x:Long = maxDelayMsecs / 2
    while(delay < 0 || delay > maxDelayMsecs)
    {
      delay = (rand.nextGaussian() * x).toLong + x
    }
    return delay

  }

}
