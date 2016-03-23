package com.github.panayiotis.spark.visualization

import com.github.panayiotis.spark.SharedSparkContext
import org.scalatest.FunSuite

class UtilsSuite extends FunSuite with SharedSparkContext  {
  
 
  test("zeppelinDagViz changes id to class") {
    val input = """<div style="display: block;" id="dag-viz-graph"><svg height="265""""
    assert(Utils.zeppelinDagViz(input).contains("class=\"dag-viz-graph\""))
    println(Utils.zeppelinDagViz(input))
  }
  

}