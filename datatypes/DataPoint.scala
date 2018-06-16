package evi_thesis.flink.datatypes

import breeze.linalg.DenseVector

sealed abstract class DataPoint[T]


case class DataVector[Double](
                       data:DenseVector[Double],
                       timestamp:Long
) extends DataPoint[Double]
{
  override def toString():String = data(0).toString + "," + data(1).toString
}

