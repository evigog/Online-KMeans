package evi_thesis.flink

import java.io.File
import com.typesafe.config.ConfigFactory
import scala.language.implicitConversions
import JobConfigurationProperties.{DefaultBufferTimeOutValue, DefaultParallelismValue,
DefaultTimeCharacteristic, DefaultNumberOfTaskRetries, DefaultDecayFactor, DefaultWatermarkInterval}
import JobConfiguration.fromStringToTimeCharacteristic

import org.apache.flink.streaming.api.TimeCharacteristic


/** Defines a flink job configuration.
  * Since checkpointing is not working well with iterative streams we will not add
  * support for it.
  *
  * @param parallelism  the default parallelism to use for the flink stream env
  * @param bufferTimeOut the buffer timeout setting
  * @param timeCharacteristic the time characteristic to use for the events
  * @param numberOfTaskRetries the number of retries for a task in case of failure
  * @param decayfactor weight of last computed model
  */
class JobConfiguration(
                        val parallelism: Int = DefaultParallelismValue,
                        val bufferTimeOut: Long = DefaultBufferTimeOutValue,
                        val timeCharacteristic: TimeCharacteristic = DefaultTimeCharacteristic,
                        val numberOfTaskRetries: Int = DefaultNumberOfTaskRetries,
                        val decayfactor: Double = DefaultDecayFactor,
                        val watermarkInterval: Long = DefaultWatermarkInterval
                        )

object JobConfigurationProperties {
  val ParallelismName = "job.parallelism"
  val BufferTimeOutName = "job.buffertimeout"
  val TimeCharacteristicName = "job.time.characteristic"
  val NumberOfTaskRetriesName = "job.task.retries"
  val DecayFactor = "job.decayfactor"
  val WatermarkInterval = "job.watermarkInterval"

  val DefaultParallelismValue = 4
  val DefaultBufferTimeOutValue = 100L
  val DefaultTimeCharacteristic = JobConfiguration.EventTimeName
  val DefaultNumberOfTaskRetries = -1
  val DefaultDecayFactor = 0.3
  val DefaultWatermarkInterval = 2 //millsec
}

object JobConfiguration{

  final val EventTimeName = TimeCharacteristic.EventTime.toString
  final val ProcessingTimeName = TimeCharacteristic.ProcessingTime.toString
  final val IngestionTimeName = TimeCharacteristic.IngestionTime.toString

  implicit def fromStringToTimeCharacteristic(value: String) : TimeCharacteristic = {
    value match {

      case EventTimeName => TimeCharacteristic.EventTime

      case ProcessingTimeName => TimeCharacteristic.ProcessingTime

      case IngestionTimeName => TimeCharacteristic.IngestionTime

      case _ => throw new IllegalArgumentException("Time characteristic unknown...")
    }
  }

  val defaultJobConfiguration = new JobConfiguration()

  /** Parses a properties file for the job.
    *
    * @param file the file to parse.
    * @return a job configuration
    */
  def parseFile(file: String): JobConfiguration = {
    val config = ConfigFactory.parseFile(new File(file))
    val parallelismProvided =
      Option(config.getInt(JobConfigurationProperties.ParallelismName))
        .getOrElse(JobConfigurationProperties.DefaultParallelismValue)
    val bufferTimeOutProvided = Option(config.getLong(JobConfigurationProperties.BufferTimeOutName))
      .getOrElse(JobConfigurationProperties.DefaultBufferTimeOutValue)
    val timeCharacteristicProvided =
      Option(config.getString(JobConfigurationProperties.TimeCharacteristicName))
      .getOrElse(JobConfigurationProperties.DefaultTimeCharacteristic)
    val numberOfTaskRetriesProvided =
      Option(config.getInt(JobConfigurationProperties.NumberOfTaskRetriesName))
      .getOrElse(JobConfigurationProperties.DefaultNumberOfTaskRetries)
    val decayFactorProvided =
      Option(config.getDouble(JobConfigurationProperties.DecayFactor))
        .getOrElse(JobConfigurationProperties.DefaultDecayFactor)
    val watermarkIntervalProvided =
      Option(config.getDouble(JobConfigurationProperties.WatermarkInterval))
        .getOrElse(JobConfigurationProperties.DefaultWatermarkInterval)


    new JobConfiguration(
      parallelism = parallelismProvided,
      bufferTimeOut = bufferTimeOutProvided,
      timeCharacteristic = timeCharacteristicProvided,
      numberOfTaskRetries = numberOfTaskRetriesProvided,
      decayfactor = decayFactorProvided
    )
  }
}
