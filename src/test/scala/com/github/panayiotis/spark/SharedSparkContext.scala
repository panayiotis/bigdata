package com.github.panayiotis.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>
  
  @transient private var _sc: SparkContext = _
  
  def sc: SparkContext = _sc
  
  var conf = new SparkConf().setMaster("local[*]").setAppName("Test")
  
  override def beforeAll() {
    super.beforeAll()
    println("initialize suite: create sparkcontext")
    _sc = new SparkContext(conf)
  }
  
  override def afterAll() {
    try {
      if (_sc != null) {
        _sc.stop()
      }
      // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
      _sc = null
    } finally {
      super.afterAll()
    }
  }
}
