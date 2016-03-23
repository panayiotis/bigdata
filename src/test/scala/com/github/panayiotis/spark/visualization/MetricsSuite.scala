package com.github.panayiotis.spark.visualization

import com.github.panayiotis.spark.SharedSparkContext
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class MetricsSuite extends FunSuite with SharedSparkContext  {
  
  def labels = Map(("Football", 1), ("Money", 11), ("Environment", 4),
                   ("Books", 13), ("UK news", 18), ("Fashion", 19),
                   ("Law", 21), ("Film", 3), ("World news", 0),
                   ("Art and design", 20), ("Sport", 9), ("Culture", 15),
                   ("Politics", 2), ("Media", 5), ("Life and style", 16),
                   ("US news", 6), ("Music", 12), ("Society", 7),
                   ("Business", 8), ("Education", 14), ("Travel", 17),
                   ("Technology", 10))
  
  def emptyMetricsCollectionFixture = new Metrics
  
  def filledMetricsCollectionFixture:Metrics = {
    val files = Seq("src/test/resources/predictionAndLabels1.tsv",
                    "src/test/resources/predictionAndLabels1.tsv",
                    "src/test/resources/predictionAndLabels3.tsv",
                    "src/test/resources/predictionAndLabels4.tsv")
    
    val a = Utils.loadPairOfDoublesRDD(sc.textFile(files(0)))
    val b = Utils.loadPairOfDoublesRDD(sc.textFile(files(1)))
    val c = Utils.loadPairOfDoublesRDD(sc.textFile(files(2)))
    val d = Utils.loadPairOfDoublesRDD(sc.textFile(files(3)))
    val metrics = new Metrics
    
    metrics.add("one",a)
      .add("two",b)
      .add("tre",c)
      .add("for",d)
  }
  test("add") {
    val metrics = emptyMetricsCollectionFixture
    assert(metrics.size == 0)
    metrics.add("a",sc.parallelize(Array((3.0,2.0),(1.0,1.0),(3.0,2.0))))
    assert(metrics.size == 1)
  }
  
  test("evaluation table starts with %table") {
    val metrics = filledMetricsCollectionFixture
    assert(metrics.zeppelinEvaluationTable.startsWith("%table"))
  }
  
  test("zeppelinRocsHtml starts with %html") {
    val metrics = filledMetricsCollectionFixture
      .setLabels(labels)
    assert(metrics.zeppelinRocsHtml.startsWith("%html"))
  }
  
  test("empty toString") {
    val metrics = new Metrics
    println(metrics)
  }
}