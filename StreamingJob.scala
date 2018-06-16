package evi_thesis.flink

import org.apache.commons.cli.ParseException
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import java.util.HashMap

trait StreamingJob {

  private def buildStreamEnv(config: JobConfiguration): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // set default env parallelism for all operators
    env.setParallelism(config.parallelism)
    // set buffer time-out
    env.setBufferTimeout(config.bufferTimeOut)
    // set eventime characteristic
    env.setStreamTimeCharacteristic(config.timeCharacteristic)
    // set number of task retries
    env.setNumberOfExecutionRetries(config.numberOfTaskRetries)
    //set interval for generating watermarks
    env.getConfig.setAutoWatermarkInterval(config.watermarkInterval) //millsec
    //set decay factor as global parameter
    val myMap = new HashMap[String, String]()
    myMap.put("job.decayfactor", config.decayfactor.toString)
    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(myMap))
    env
  }

  def run(env: StreamExecutionEnvironment)

  def main(args: Array[String]): Unit = {

    try {
      val fileOpt = ParseCLI.parseArgs(args)
      val config = {
        if(fileOpt.isDefined) {
          JobConfiguration.parseFile(fileOpt.get)
        } else {
          JobConfiguration.defaultJobConfiguration
        }
      }
      run(buildStreamEnv(config))
    }catch {
      case e: ParseException => e.printStackTrace()
      case e: Exception => e.printStackTrace()
    }
  }
}
