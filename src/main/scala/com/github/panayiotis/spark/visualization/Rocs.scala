package com.github.panayiotis.spark.visualization

import org.apache.spark.rdd.RDD

object Rocs {
  def apply(rocs: Array[RDD[(Double, Double)]],labels:Map[String,Int]) = {
    val swapedLabels = labels.map(_.swap)
    
    val rocsjson = rocs.zipWithIndex.map{
      case (roc,index) => {
        val cat = swapedLabels(index)
        //val cat = "Svm"
        //var ratio = 1.0
        //val count = roc.count
        //if(count > 100 ){ratio = 100/count}
        
        roc.map{
          p => {
            val x = p._1.toString
            val y = p._2.toString
            s"    [$x, $y]"
          }
        }
          //.sample(true, ratio, 1)
          .collect
          .mkString(s"""{ "name": "$cat", "values": [\n""",",\n","]}")
      }
    }.mkString("[", ",\n","]")
  }
}