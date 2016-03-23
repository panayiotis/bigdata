package com.github.panayiotis.spark.visualization

import java.io.{File, FileOutputStream, PrintWriter}
import breeze.linalg.{SparseVector=>BreezeSparseVector}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by panos on 28/2/2016.
  */
object Utils {
  def reset = println("\033\143")
  
  def write(filename: String, content: String) = {
    val file = new File("data/" + filename)
    val printwriter = new PrintWriter(file)
    try {
      printwriter.write(content)
    } finally {
      printwriter.close()
    }
  }
  
  def append(filename: String, content: String) = {
    val file = new File("data/" + filename)
    val filestream = new FileOutputStream(file, true)
    val printwriter = new PrintWriter(filestream)
    try {
      printwriter.append(content)
    } finally {
      printwriter.close()
    }
  }
  
  def toBreeze(v:SparseVector):BreezeSparseVector[Double] = {
    //assert(v.size == 1048576, {println("toBreeze: Wrong vector size " + v.size)})
    new BreezeSparseVector[Double](v.indices, v.values, v.size)
  }
  
  def fromBreeze(bv: BreezeSparseVector[Double]):SparseVector = {
    val size = bv.size
    //assert(size == 1048576, {println("fromBreeze: Wrong vector size " + size)})
    val indices = bv.array.index
    val data = bv.data
    new SparseVector(size, indices, data).toSparse
  }
  
  def transformRDDToLabeledPoint(rdd: RDD[(String, Seq[String])],
                                 index:Map[String,Int]) = {
    val size = index.size
    val tokens = index.keys.toSet
    rdd.map{
      case(label, seq) => {
        val (indices,values) = seq
          .filter(tokens.contains(_))
          .map ( word => (index(word).toInt,1) )
          .groupBy(_._1)
          .map(p => (p._1, p._2.size.toDouble))
          .toSeq
          .sortBy(_._1)
          .unzip
        LabeledPoint(label.toInt,new SparseVector(size, indices.toArray, values.toArray))
      }
    }
  }
  
  def writePairOfDoublesRDD(filename:String,
                            predictionAndLabels:RDD[(Double,Double)]): Unit = {
    val content = predictionAndLabels
      .map(p => s"${p._1}\t${p._2}")
      .collect
      .mkString("\n")
    val file = new File(filename)
    val printwriter = new PrintWriter(file)
    try {
      printwriter.write(content)
    } finally {
      printwriter.close()
    }
  }
  def loadPairOfDoublesRDD(rdd:RDD[String]): RDD[(Double,Double)] = {
    rdd.map(line=>line.split('\t'))
      .map{case(Array(a,b)) => (a.toDouble,b.toDouble)}
  }
  
  def zeppelinDagViz(dagdiv:String):String ={
    val css = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/dag.css")).getLines.mkString
    val html = new StringBuilder
    html.append("%html\n")
      .append(s"""<style>\n$css\n</style>\n""")
      .append(dagdiv.replace("id=\"dag-viz-graph\"","class=\"dag-viz-graph\""))
      .toString()
  }
}
