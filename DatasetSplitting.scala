package evi_thesis.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.preprocessing.Splitter
import org.apache.flink.api.scala._

//splitting dataset into training set and test set, based on fraction parameter (0.5 now)

object DatasetSplitting {

  def main(args: Array[String]): Unit =
  {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val parameter = ParameterTool.fromArgs(args)
    val inputFile = parameter.get("filePath")
    val dataset = env.readTextFile(inputFile)
    val splitting = Splitter.trainTestSplit[String](dataset, 0.5, true)

    val training = splitting.training.map(l=>new Tuple1(l))
    training.writeAsCsv("./Results/Splitting/spambase_training.data")
    training.map(new AssignID()).writeAsCsv("./Results/Splitting/Training_Labels.data")

    val testing =splitting.testing.map(l=>new Tuple1(l))
    testing.writeAsCsv("./Results/Splitting/spambase_testing.data")
    testing.map(new AssignID()).writeAsCsv("./Results/Splitting/Testing_Labels.data")



    env.execute()


  }

}

class AssignID extends MapFunction[Tuple1[String], Tuple2[Int, Int]]
{
  private var id = 0

  override def map(in:Tuple1[String]):Tuple2[Int, Int] =
  {
    id = id + 1
    new Tuple2(id, in._1.split(",")(57).toInt)
  }
}