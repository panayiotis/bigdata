package com.github.panayiotis.spark.examples
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.github.panayiotis.spark.classification.MulticlassSVMWithSGD
import com.github.panayiotis.spark.visualization.Metrics
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, 
NaiveBayes}
import org.apache.spark.rdd.RDD

/* SimpleApp.scala */
object Classification{
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RandomForests")
    val sc = new SparkContext(conf)
    
    /* load dataset */
    val sample=0.5
    val data: RDD[LabeledPoint] = sc.objectFile[LabeledPoint]("data/rdd/train/LabeledPoint/wordbags")//.sample(true,0.1,1)
    val labels = sc.objectFile[(String, Int)]("data/rdd/labels").collect.toMap
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (training, test) = (splits(0), splits(1))
    training.cache
     val numClasses = data.map(_.label).max().toInt + 1
    
    val data2: RDD[LabeledPoint] = sc.objectFile[LabeledPoint]("data/rdd/train/LabeledPoint/tf")//.sample(true,0.1,1)
    val splits2 = data2.randomSplit(Array(0.7, 0.3))
    val (training2, test2) = (splits2(0), splits2(1))
    training2.cache
    
    /* define classification methods */
    def svmwb = {
      val numIterations = 1
      val description = s"SVMWordbags iterations: $numIterations"
      println(description)
      val model = MulticlassSVMWithSGD.train(training2, numIterations)
      val predictionAndLabels = model.predict(test2)
      (description, predictionAndLabels)
    }
    
    def svmtf = {
      val numIterations = 1
      val description = s"SVMTFIDF iterations: $numIterations"
      println(description)
      val model = MulticlassSVMWithSGD.train(training, numIterations)
      val predictionAndLabels = model.predict(test)
      (description, predictionAndLabels)
    }
    
    def rf = {
      // Empty categoricalFeaturesInfo indicates all features are continuous.
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 3 // Use more in practice.
      val featureSubsetStrategy = "auto" // Let the algorithm choose.
      val impurity = "gini"
      val maxDepth = 4
      val maxBins = 32
      val description = s"RandomForest trees: $numTrees depth: $maxDepth bins: $maxBins"
      println(description)
  
      val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
      
      val predictionAndLabels = test.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      (description, predictionAndLabels)
    }
    
    def nb = {
      val description = s"NaiveBayes"
      println(description)
      val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
      
      val predictionAndLabels = test.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      (description, predictionAndLabels)
    }
    
    def lnr = {
      val numIterations = 10
      val description = s"LinearRegressionWithSGD iterations: $numIterations"
      println(description)
      val model = LinearRegressionWithSGD.train(training, numIterations)
      
      val predictionAndLabels = test.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      (description, predictionAndLabels)
    }
    
    def lgr = {
      val description = s"LogisticRegressionWithLBFGS"
      println(description)
      val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(numClasses)
        .run(training)
      
      val predictionAndLabels = test.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      (description, predictionAndLabels)
    }
    
    /* run classification methods */
    val metrics = Metrics().setLabels(labels)
  
    metrics.add(svmtf)
    training2.unpersist(true)
  
    metrics.add(svmwb).add(rf)
      .add(nb)
      .add(lnr)
      .add(lgr)
    
    println(metrics)
  
    println(metrics.zeppelinEvaluationTable)
    
    //println(metrics.zeppelinRocsHtml)
  
    //println(metrics.zeppelinRocHtml)
    
    println(metrics.rocsjson)
  
    /* Write predictionAndLabels to files for testing fixtures */
    /*
    Utils.writePairOfDoublesRDD("src/test/resources/predictionAndLabels1.tsv",rf._2)
    Utils.writePairOfDoublesRDD("src/test/resources/predictionAndLabels2.tsv",nb._2)
    Utils.writePairOfDoublesRDD("src/test/resources/predictionAndLabels3.tsv",lnr._2)
    Utils.writePairOfDoublesRDD("src/test/resources/predictionAndLabels4.tsv",lgr._2)
    */
  }
}