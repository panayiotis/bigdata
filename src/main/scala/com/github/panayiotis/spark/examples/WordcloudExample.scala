package com.github.panayiotis.spark.examples

import com.github.panayiotis.spark.visualization.Utils._
import com.github.panayiotis.spark.visualization.Wordcloud
import org.apache.spark.{SparkConf, SparkContext}

object WordcloudExample {
  
  def main(args: Array[String]) {
    
    println("Wordcloud example")
    
    val conf = new SparkConf()
      .setAppName("Wordcloud")
    
    val sc = new SparkContext(conf)
    
    // load train dataset
    Datasets.sc = Some(sc)
    val data = Datasets.train(.001).cache
    
    // create wordclouds
    val model = Wordcloud.create(data,100,0.5)
  
    //write("wordcloud.html",model.zepelinWordcloudHtml)
    
    write("cloudsjson.js","window.clouds =" + model.json)
    write("categories.yaml",model.categoriesyaml)
  
    
    println("finished")
  }
  

}
