package com.github.panayiotis.spark.examples

import java.io.Serializable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Datasets extends Serializable {
  var sc = None: Option[SparkContext]
  var trainfile = "data/train.csv"
  var trainset = None: Option[RDD[(String, Seq[String])]]
  var testfile = "data/test.csv"
  var testset = None: Option[RDD[(String, Seq[String])]]
  
  def train:RDD[(String, Seq[String])] = {
    trainset.getOrElse{
      trainset = Some(this.apply(trainfile))
    }
    trainset.get
  }
  
  def train(sample:Double):RDD[(String, Seq[String])] = {
    train.sample(true,sample,1)
  }
  
  
  def test:RDD[(String, Seq[String])] = {
    testset.getOrElse{
      testset = Some(this.apply(testfile))
    }
    testset.get
  }
  
  
  def test(sample:Double):RDD[(String, Seq[String])] = {
    test.sample(true,sample,1)
  }
  
  def apply (rdd: RDD[String]):RDD[(String, Seq[String])] = {
    val id = 0
    val title = 1
    val text = 2
    val cat = 3 
    var label = 0
    rdd.map{
      line=> {
        val array = line.split('\t')
        if (array.length == 4){
          label = cat
        }else if (array.length == 3){
          label = id
        }else{
          assert(false,{println("corrupted dataset")})
        }
        
        ( 
          array(label),
          tokenize(array(title)) ++ tokenize(array(text))
        )
      }
    }
  }
  
  def apply (filename: String):RDD[(String, Seq[String])] = {
   // val rdd:RDD[String] = sc.textFile(filename)
    apply(sc.get.textFile(filename))
  }
  
  def apply (filename: String, sample:Double):RDD[(String, Seq[String])] = {
    apply(sc.get.textFile(filename).sample(true,sample,1))
  }
  
  def tokenize(line: String): Seq[String] = {
    val regex = """[^0-9]*""".r
    line.split("""\W+""").
    map(_.toLowerCase).
    filter(token => regex.pattern.matcher(token).matches).
    filter(token => token.length >= 2).
    toSeq
  }
  
}
