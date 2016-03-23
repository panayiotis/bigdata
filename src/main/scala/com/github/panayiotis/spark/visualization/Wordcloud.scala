package com.github.panayiotis.spark.visualization

import java.io._
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import breeze.linalg.{SparseVector => BreezeSparseVector}
import org.apache.spark.mllib.feature.IDF

class WordcloudModel (private val cloudsWithoutStopwords:Array[(String, Array[(String, Int)])],
                      private val numWords: Int,
                      private val categoriesWithCounts: scala.collection.Map[String,Long])
  extends Logging with Serializable {
  
  /**
    * Returns the json string.
    */
  def json = {
    /* JSON */
    logInfo("create json")
    cloudsWithoutStopwords.map { p =>
      (p._1,
        p._2.take(numWords).map {
          p => {
            val text = p._1
            val size = p._2.toFloat
            s"""{"text": "$text", "size": $size }"""
          }
        }
        )}.map {
      p => {
        val cat = p._1
        val count = categoriesWithCounts(p._1)
        val words = p._2.mkString("[", ",\n  ", "]")
        s"""{"category": "$cat",\n"count":$count,\n"words":$words}"""
      }
    }.mkString("[", ",\n", "]")
  }
  
  /**
    * Returns the categories yaml.
    */
  def categoriesyaml = {
    categoriesWithCounts.toSeq
      .sortBy(-_._2)
      .map(p => p._1 + ": " + p._2 + "\n").mkString("")
  }
  
  /**
    * Returns the categories in tsv appropriate for zeppelin %table.
    */
  def categoriestable = {
    categoriesWithCounts.toSeq.sortBy(-_._2)
      .map(p => p._1 + "\t" + p._2)
      .mkString("category\tcounts\n","\n","\n")
  }
  
  /**
    * Returns Categories in zeppelin %html.
    */
  def categorieszepelin:String = s"%table ${categoriestable}"
  
  /**
    * Returns wordclouds in zeppelin %html.
    */  
  def zepelinWordcloudHtml = {
    val js = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/wordcloud.js")).getLines.mkString("\n")
    val css = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/wordcloud.css")).getLines.mkString("\n")
    val html = new StringBuilder
    html.append(s"""%html\n""")
    html.append(s"""<div id="tabs"></div>\n""")
    html.append(s"""<style>\n$css\n</style>\n""")
    html.append(s"""<script>\nvar clouds = ${json};\n$js\n</script>\n""")
    html.toString()
  }
}

/**
  * Build the Wordcloud
  */
class Wordcloud private(private var numWords: Int,
                        private var stopwordsRatio: Double)
  extends Logging with Serializable {
  
  /**
    * Build the word cloud
    * Returns a WordcloudModel
    * */
  def build( input: RDD[(String, Seq[String])]) = {
    
    /* Generate Categories*/
    logInfo("create categories")
    val categoriesWithCounts = input.countByKey
    
    val text: RDD[Seq[String]] = input.values
    val tokens: Seq[String] = text.flatMap(l => l).distinct.collect.toSeq
    logInfo(s"${tokens.length} tokens in dataset")
    val tokensHashes: Map[String, Int] = tokens.zipWithIndex.toMap
    val hashesTokens: Map[Int, String] = tokensHashes.map(_.swap)
    val vectorsize = tokens.length
    
    /* TF */
    logInfo("create Term Frequency vector")
    def transformTF(seq: Seq[String]): SparseVector = {
      val (indices, values) = seq.map(word => (tokensHashes(word), 1))
        .groupBy(_._1)
        .map(p => (p._1, p._2.size.toDouble))
        .toSeq
        .sortBy(_._1)
        .unzip
      new SparseVector(vectorsize, indices.toArray, values.toArray)
    }
    
    val texttf: RDD[Vector] = text.map(transformTF)
    texttf.cache
    val idf = new IDF().fit(texttf)
    texttf.unpersist(false)
    
    /* Stopwords*/
    logInfo("create richwords")
    val numRichwords = tokens.length - (tokens.length * stopwordsRatio).toInt // five percent
    val vector = idf.transform(transformTF(tokens)).toSparse
    val richwords = vector.indices.zip(vector.values).toSeq.sortBy(-_._2)
      .map(p=>hashesTokens(p._1))
      .take(numRichwords)
    //println(richwords.mkString(" "))
    
    /* clouds */
    logInfo("create clouds")
    val clouds = input.
      mapValues(v => toBreeze(transformTF(v).toSparse)).
      reduceByKey(_ + _).
      mapValues {
        vector => {
          vector.index.zip(vector.data).
            map {
              p => (hashesTokens(p._1), p._2.toInt)
            }.sortBy(-_._2)
        }
      }.collect
    
    val cloudsWithoutStopwords: Array[(String, Array[(String, Int)])] = clouds.map(p => (p._1, p._2.filter {
      case (pair) => richwords.contains(pair._1)
    }))
    
    /* Returns WordcloudModel */
    new WordcloudModel(cloudsWithoutStopwords,numWords,categoriesWithCounts)
  }
  
  private def toBreeze(v: SparseVector): BreezeSparseVector[Double] = {
    new BreezeSparseVector[Double](v.indices, v.values, v.size)
  }
  
  /**
    * Construct a Wordcloud object with default parameters: {numWords: 100, 
    * stopwordsRatio: 2%,
    */
  def this() = this(100, 0.02)
  
}

/**
  * Top-level methods for calling Wordcloud.
  */
object Wordcloud {
  
  /**
    * Build a Wordcloud model given an RDD of (category, text) pairs.
    *
    * @param input         RDD of (category:String, text:Seq[String]) pairs.
    */
  def create(input: RDD[(String, Seq[String])]
           ): WordcloudModel = {
    new Wordcloud().build(input)
  }
  
  def create(input: RDD[(String, Seq[String])],
            numWords:Int,
            stopwordsRatio:Double): WordcloudModel = {
    new Wordcloud(numWords,stopwordsRatio).build(input)
  }
}
