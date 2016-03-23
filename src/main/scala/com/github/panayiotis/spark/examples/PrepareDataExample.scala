package com.github.panayiotis.spark.examples

import com.github.panayiotis.spark.visualization.Utils._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Paths, Files}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF

object PrepareDataExample {
  
  def main(args: Array[String]) {
    
    if( false // Files.exists(Paths.get("data/rdd"))
    ){
      println("path data/rdd already exist")
      sys.exit(1)
    }
    
    val conf = new SparkConf()
      .setAppName("Preprocess")
    val sc = new SparkContext(conf)
    
    /* load dataset */
    Datasets.sc = Some(sc)
    val sample = .1
    val unproccessed: RDD[(String, Seq[String])] = Datasets.train(sample)
    
    val preprocess = new Preprocess(unproccessed)
    
    /* labels */
    println("save labels rdd")
    val labels = preprocess.labels
    sc.parallelize(labels.toSeq).saveAsObjectFile("data/rdd/labels")
    
    /* TF with hashing trick */
    val hashingTF = new HashingTF(1000)
    unproccessed.map{
      case(label,seq) => {
        LabeledPoint(labels(label), hashingTF.transform(seq).toSparse)
      }
    }.saveAsObjectFile("data/rdd/train/LabeledPoint/tf1000")
    
    /* vectors */
    println("save vectors rdd")
    preprocess.vectors.saveAsObjectFile("data/rdd/train/SparseVector/tf")

    /* labeled points */
    println("save labeledpoints rdd")
    preprocess.labeled.saveAsObjectFile("data/rdd/train/LabeledPoint/tf")

    transformRDDToLabeledPoint(Datasets.test,preprocess.index)
      .saveAsObjectFile("data/rdd/test/LabeledPoint/tf")

    /* wordbags */
    println("preprocess with wordbags")
    
    println("save wordbags rdd")
    preprocess.wordbags.saveAsObjectFile("data/rdd/train/LabeledPoint/wordbags")
    
    /* Load */
    //val l = sc.objectFile[(String, Int)]("data/rdd/trainrdd/labels").collect.toMap
    
  }
}
