package com.github.panayiotis.spark.visualization

import com.github.panayiotis.spark.SharedSparkContext
import com.github.panayiotis.spark.clustering.KMeans
import com.github.panayiotis.spark.examples.{Preprocess, Datasets}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class KMeanSuite extends FunSuite with SharedSparkContext  {
  private var trainrdd = None: Option[RDD[(Int, SparseVector)]]
  private var labels= None: Option[Map[String, Int]]
  private val maxIterations = 1
  
  override def beforeAll() {
    super.beforeAll()
    println("initialize suite: load dataset")
    Datasets.sc = Some(sc)
    val unproccessed = Datasets.train(.01)
    val preprocess = new Preprocess(unproccessed)
    trainrdd = Some(preprocess.vectors.cache)
    labels = Some(preprocess.labels)
  }
  
  override def afterAll() {
    trainrdd.get.unpersist()
    super.afterAll()
  }
  
  test("cluster") {
    assert(KMeans.cluster(trainrdd.get.values.takeSample(true,10,3),trainrdd.get.values.first)<10)
  }
  
  test("Ensure all SparseVectors have the same size") {
    val size = trainrdd.get.first._2.size
    assert(trainrdd.get.map(_._2.size).filter{s => s != size}.count == 0)
  }
  
}