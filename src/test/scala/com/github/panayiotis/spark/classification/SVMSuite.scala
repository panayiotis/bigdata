package com.github.panayiotis.spark.classification

import com.github.panayiotis.spark.SharedSparkContext
import com.github.panayiotis.spark.visualization.Metrics
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.scalatest.FunSuite
//import org.apache.spark.mllib.classification.{MulticlassSVMModel, MulticlassSVMWithSGD}

class SVMSuite extends FunSuite with SharedSparkContext  {
  
  test("add") {
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/sample_libsvm_data.txt")
    
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    
    // Run training algorithm to build the model
    val numIterations = 100
    val model1 = SVMWithSGD.train(training, numIterations)
    val model2 = MulticlassSVMWithSGD.train(training, numIterations)
  
  
    // Compute raw scores on the test set
    val predictionAndLabels1 = test.map { case LabeledPoint(label, features) =>
      val prediction = model1.predict(features)
      (prediction, label)
    }
    val predictionAndLabels2 = model2.predict(test)
  
    // Instantiate metrics object
    val metrics1 = new MulticlassMetrics(predictionAndLabels1)
    
    // Get evaluation metrics.
    val metrics2 = Metrics().add("test",predictionAndLabels2).toSeq.head
    assert(metrics1.fMeasure == metrics2.fMeasure)
    assert(metrics1.precision == metrics2.precision)
    assert(metrics1.recall == metrics2.recall)
  }
}