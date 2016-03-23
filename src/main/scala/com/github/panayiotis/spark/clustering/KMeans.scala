package com.github.panayiotis.spark.clustering

import breeze.linalg.{SparseVector=>BreezeSparseVector}
import com.github.panayiotis.spark.visualization.Utils._
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD

object KMeans{
  
  /* Return the center index given a vector */
  def cluster ( centers: Array[SparseVector],vector: SparseVector): Int = {
    val dist = centers.map(center => Vectors.sqdist(center,vector))
    dist.zipWithIndex.min._2
  }
  
  /* Compute centers from a PairRDD */
  def centers(rdd:RDD[(Int, SparseVector)]):Array[SparseVector] = {
    val vectorsize = rdd.first._2.size
    val zero = (new BreezeSparseVector[Double](Array(), Array(), vectorsize), 0)
    val seqOp =  (a:(BreezeSparseVector[Double],Int), b:BreezeSparseVector[Double]) =>  {
      (a._1 + b, a._2 + 1 )
    }
    val combOp = (a:(BreezeSparseVector[Double],Int), b:(BreezeSparseVector[Double],Int)) =>  {
      (a._1 + b._1, a._2 + b._2 )
    }
    
    val breezerdd = rdd.mapValues(v=>toBreeze(v))
      .aggregateByKey(zero)(seqOp, combOp)
      .values
      .map(v => v._1.mapValues(_/v._2) )
    
    breezerdd.map(fromBreeze)
      .collect
  }
  
  /* Collect results */
  def results(trainrdd: RDD[(Int, SparseVector)],
              resultrdd:RDD[(Int, SparseVector)],
              labels:Map[String,Int]):String = {
    
    val resultcenters = centers(resultrdd)
    
    val csvHeader = labels
      .toSeq.sortBy(_._2)
      .map(_._1)
      .mkString("","\t","\n")
    
    val zero = collection.mutable.Map[Int,Double]()
    for( i <- 0 to trainrdd.keys.max){zero(i)=0;}
    
    val seqOp =  (map: scala.collection.mutable.Map[Int,Double],
                  value:Int) =>  {
      map(value)+=1
      map
    }
    
    val combOp = (map1: scala.collection.mutable.Map[Int,Double],
                  map2:scala.collection.mutable.Map[Int,Double]) => {
      map1.transform((key, value) => value+map2(key))
    }
    
    trainrdd.map( p => (cluster(resultcenters,p._2.asInstanceOf[SparseVector]), p._1))
      .aggregateByKey(zero)(seqOp, combOp)
      .mapValues{
        map => {
          val sum = map.foldLeft(0.0)(_+_._2)
          map.transform((key,value) => (value/sum * 100).round / 100.toDouble)
        }
      }
      .values
      .map(map => map.toSeq.sortBy(_._1).map(_._2).mkString("\t"))
      .collect
      .mkString(csvHeader,"\n","")
  }
  
  def run(rdd:RDD[(Int, SparseVector)],
          sample:Double = 1.0): RDD[(Int, SparseVector)] = {
    
    var initialCenters:Array[SparseVector] = Array[SparseVector]()
    
    /* Compute initial centers from trainrdd */
    if (sample < 1.0) {
      initialCenters = centers{
        rdd.groupByKey()
          .map(p => (p._1, p._2.head))
          .union(rdd.sample(withReplacement = true,sample,1))
      }
    }
    else{
      initialCenters = centers(rdd)
    }
    
    rdd.map( p => (cluster(initialCenters,p._2.asInstanceOf[SparseVector]), p._2))
      .cache
  }
  
}
