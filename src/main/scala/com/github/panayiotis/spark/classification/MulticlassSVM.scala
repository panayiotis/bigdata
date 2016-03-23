/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.panayiotis.spark.classification

import java.io.Serializable

import org.apache.spark.Logging
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD

/**
  * Model for Support Vector Machines (SVMs).
  *
  */
class MulticlassSVMModel(models:Seq[SVMModel])
  extends Logging with Serializable {
  
  def predict(point: LabeledPoint): Double = {
    models.zipWithIndex.map{
      case (model,index) => {
        (model.predict(point.features), index)
      }
    }.maxBy { case (score, index) => score }
    ._2
  }
  
  def predict(input: RDD[LabeledPoint]): RDD[(Double,Double)] = {
    input.map(p => (predict(p), p.label) )
  }
}

/**
  * Train a Support Vector Machine (SVM) using Stochastic Gradient Descent. By default L2
  * regularization is used, which can be changed via SVMWithSGD.optimizer.
  * NOTE: Labels used in SVM should be {0, 1}.
  */

class MulticlassSVMWithSGD private ( private var stepSize: Double,
                                     private var numIterations: Int,
                                     private var regParam: Double,
                                     private var miniBatchFraction: Double)
  extends Logging with Serializable {
  
  def run(input:RDD[LabeledPoint]) = {
    val maxLabel = input.map ( l => l.label ).collect.max.toInt
    
    val models = (0 to maxLabel).map {
      index => {
        logInfo(s"training model for class $index")
        //println(training.filter{case LabeledPoint(label, features) => {label==index}}.count)
        val projectionrdd: RDD[LabeledPoint] = input.map {
          case LabeledPoint(label, features) => {
            LabeledPoint(if (label == index) 1.0 else 0.0, features)
          }
        }.cache()
        
        val model = SVMWithSGD.train(projectionrdd, numIterations, stepSize, regParam, miniBatchFraction)
        
        projectionrdd.unpersist(false)
        
        model.clearThreshold()
        model
      }
    }.toSeq
    
    new MulticlassSVMModel(models)
  }
}

/**. NOTE: Labels us
  * Top-level methods for calling SVM.
  */
object MulticlassSVMWithSGD {
  
  /**
    * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
    * of iterations of gradient descent using the specified step size. Each iteration uses
    * `miniBatchFraction` fraction of the data to calculate the gradient.
    *
    * @param input             RDD of (label, array of features) pairs.
    * @param numIterations     Number of iterations of gradient descent to run.
    * @param stepSize          Step size to be used for each iteration of gradient descent.
    * @param regParam          Regularization parameter.
    * @param miniBatchFraction Fraction of data to be used per iteration.
    */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double): MulticlassSVMModel = {
    new MulticlassSVMWithSGD(stepSize, numIterations, regParam, miniBatchFraction).run(input)
  }
  
  /**
    * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
    * of iterations of gradient descent using the specified step size. We use the entire data set to
    * update the gradient in each iteration.
    *
    * @param input         RDD of (label, array of features) pairs.
    * @param stepSize      Step size to be used for each iteration of Gradient Descent.
    * @param regParam      Regularization parameter.
    * @param numIterations Number of iterations of gradient descent to run.
    * @return a SVMModel which has the weights and offset from training.
    */
  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double): MulticlassSVMModel = {
    train(input, numIterations, stepSize, regParam, 1.0)
  }
  
  /**
    * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
    * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
    * update the gradient in each iteration.
    *
    * @param input         RDD of (label, array of features) pairs.
    * @param numIterations Number of iterations of gradient descent to run.
    * @return a SVMModel which has the weights and offset from training.
    */
  def train(input: RDD[LabeledPoint], numIterations: Int): MulticlassSVMModel = {
    train(input, numIterations, 1.0, 0.01, 1.0)
  }
}