package com.github.panayiotis.spark.examples

import com.github.panayiotis.spark.visualization.{ Rocs}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


/* SimpleApp.scala */
object SVMWithWordBags{

  def main(args: Array[String]) {
    println("hello again")
    val conf = new SparkConf().setAppName("SVMWithWordBags")
    val sc = new SparkContext(conf)
    Datasets.sc = Some(sc)
    
    
    val sample = 0.01
    val unproccessed = Datasets.train(sample)
    val preprocess = new Preprocess(unproccessed)
    val data = preprocess.wordbags.cache
    val labels = preprocess.labels
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (training, test) = (splits(0), splits(1))
    training.cache
    val numClasses = data.map(_.label).max().toInt + 1

    val numIterations = 3

    val description = s"SVMWithWordBags sample: ${sample*100}%, iterations: $numIterations"
    println(description)
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

    //def predict(models: Array[SVMModel],
    //            data: RDD[LabeledPoint]): RDD[Double] = {
    //  data.map( point => predictOne(models, point))
    //}

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
    
    Rocs(rocs,labels)
    
    // Compute raw scores on the test set
    val predictionAndLabels = test.map { point =>
      val prediction = predict(models,point)
      (prediction, point.label)
    }

    //Metrics(description,predictionAndLabels, labels,test.count)

  }
}

