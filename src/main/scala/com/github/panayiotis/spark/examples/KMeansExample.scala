package com.github.panayiotis.spark.examples

import com.github.panayiotis.spark.clustering.KMeans
import com.github.panayiotis.spark.visualization.Utils._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import concurrent._
import ExecutionContext.Implicits._

object KMeansExample{
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("KMeans")
    val sc = new SparkContext(conf)
    
    /* load dataset */
    val trainrdd: RDD[(Int, SparseVector)] = 
      sc.objectFile[(Int, SparseVector)]("data/rdd/train/SparseVector/tf").sample(true,0.05,1)
    val labels = sc.objectFile[(String, Int)]("data/rdd/labels").sample(true,0.05,1)
      .collect.toMap
    val maxIterations = 1
    
    /* run kmeans */
    var tmp = trainrdd
    for (iteration <- 1 to maxIterations){
      println(s"iteration $iteration of $maxIterations")
      tmp = KMeans.run(rdd=tmp)
    }
  
    sys.exit(0)
    
    /* results */
    val csv = KMeans.results(trainrdd,tmp,labels)
    println("write output file clustering_KMeans.csv")
    write("clustering_KMeans.csv", csv)
    print(s"%table $csv")
    
    /* paint table data */
    /*
    print("""%html
    <script>
    $("td").filter(function(){ return $(this).text() === '0.0';}).css('color','grey');
    $("td").filter(function(){ return parseFloat($(this).text()) > 0.2;}).css('color','orange');
    $("td").filter(function(){ return parseFloat($(this).text()) > 0.5;}).css('color','red');
    </script>
    """)
    */
  }
}
