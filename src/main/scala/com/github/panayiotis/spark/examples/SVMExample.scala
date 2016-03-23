package com.github.panayiotis.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.github.panayiotis.spark.visualization.Rocs
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import com.github.panayiotis.spark.visualization.Utils._


object SVMExample{

  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("SVMExample")
    val sc = new SparkContext(conf)
    
    val sample=1.0
    val data: RDD[LabeledPoint] = 
      sc.objectFile[LabeledPoint]("data/rdd/train/LabeledPoint/tf")//.sample(true,0.1,1)
    val labeledTestSet: RDD[LabeledPoint] =
      sc.objectFile[LabeledPoint]("data/rdd/test/LabeledPoint/tf")//.sample(true,0.1,1)
    val labels = sc.objectFile[(String, Int)]("data/rdd/labels").collect.toMap
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (training, test) = (splits(0), splits(1))
    training.cache
    val numClasses = data.map(_.label).max().toInt + 1
    val numIterations = 3
    
    val description = s"SVM TFIDF, iterations: $numIterations"
    println(description)
    
    /* create SVM one vs many models */
    println("create models")
    val models = (0 until numClasses).map { index =>
      println(s"training model for class $index")
      //println(training.filter{case LabeledPoint(label, features) => {label==index}}.count)
      val projectionrdd: RDD[LabeledPoint] = training.map {
        case LabeledPoint(label, features) => {
          LabeledPoint(if (label == index) 1.0 else 0.0, features)
        }
      }.cache()
      
      val model = SVMWithSGD.train(projectionrdd, numIterations)
      projectionrdd.unpersist(false)
      
      model.clearThreshold()
      model
    }.toArray
    
    /* create a collection of binary metrics */
    
    def predictOne(models: Array[SVMModel],
                point: LabeledPoint): Double = {
      models
      .zipWithIndex
      .map{
        case (model,index) =>
          (model.predict(point.features),index)
      }.maxBy { case (score, index) => score}
      ._2
    }
    
    def predict(models: Array[SVMModel],
                point: LabeledPoint): Double = {
      predictOne(models,point)
    }
    
    val binarymetrics = models.map{ model =>
      val binaryScoreAndLabels = test.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }
      new BinaryClassificationMetrics(binaryScoreAndLabels)
    }
    
    val rocs = binarymetrics.map(metrics => metrics.roc)
    
    /* Rocs */
    Rocs(rocs,labels)
    
    /* Compute raw scores on the test set */
    val predictionAndLabels = test.map { point =>
      val prediction = predict(models,point)
      (prediction, point.label)
    }
    
    //Metrics(description,predictionAndLabels, labels,test.count)
    
    /* create csv */
    val swapedLabels = labels.map(_.swap)
    val csv = labeledTestSet.map { point =>
      val prediction = predict(models,point)
      s"${point.label.toInt}\t${swapedLabels(prediction.toInt)}"
    }.collect.mkString("ID\tPredicted Category\n","\n","")
    
    write("SVM_TFIDF_testSet_Categories.csv",csv)
  }
}

