package evi_thesis.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/** Simple DataSet example
  *
  */
object SimpleStreaming extends StreamingJob{

  //  TODO: move this to integration tests
  override def run(env: StreamExecutionEnvironment): Unit = {
    val words: DataStream[String] = env.fromElements[String]("Hello", "World!")
    val res = words.map(_.toUpperCase)
    res.print()  // print calls execute no need to call it explicitly
    env.execute("Simple Streaming")
  }
}


