package evi_thesis.flink.datatypes



case class Centroid(center:DataVector[Double], cluster:Int, size:Double, round:Int) //each round -> one window of trainingStream








