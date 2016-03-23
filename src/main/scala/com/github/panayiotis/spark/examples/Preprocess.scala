package com.github.panayiotis.spark.examples

import java.io.Serializable
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.feature.IDFModel
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

class Preprocess(rdd: RDD[(String, Seq[String])]) extends Serializable {
  
  var index          = Map[String,Int]()
  var tokens         = Set[String]()
  var size           = 0
  var _vectors       = None: Option[RDD[(Int,SparseVector)]]
  def vectors        = _vectors.getOrElse { this.constructVectors; _vectors.get }
  var _idf           = None: Option[IDFModel]
  def idf            = _idf.get
  var _labels        = None: Option[collection.immutable.Map[String,Int]]
  def labels         = _labels.getOrElse { this.constructLabels; _labels.get }
  var categoryCounts = collection.Map[String,Long]()
  
  var _wordbags      = None: Option[RDD[LabeledPoint]]
  var _wordbagVectors= None: Option[RDD[Vector]]
  var word2vec       = None: Option[Word2Vec]
  var word2vecmodel  = None: Option[Word2VecModel]
  def wordbags       = _wordbags.getOrElse {
    this.constructWordbags;
    _wordbags.get
  }
  
  def constructVectors = {
    println("construct vectors")
    
    tokens = rdd.values.flatMap(l=>l).distinct.collect.toSet
    
    index = tokens.zipWithIndex.toMap
    
    size = tokens.size
    
    val initialtf = rdd.values.map(seq => transform(seq,index,size).compressed)
    
    val initialidf = new IDF().fit(initialtf)
    
    val tokenstfidf = initialidf.transform(transform(tokens.toSeq,index,size)).toSparse
    
    val tokenstfidfIndex = tokenstfidf.indices.zip(tokenstfidf.values).toMap
    
    val tokensIdfScores = index.map{
      p => (p._1, tokenstfidfIndex.getOrElse(p._2.toInt,0.0).toDouble)
    }
    
    index = tokensIdfScores.toSeq.sortBy(-_._2).map(p=>p._1).zipWithIndex.toMap
    
    val tf = rdd.values.map(seq=>transform(seq,index,size).compressed)
    
    val idf = new IDF().fit(tf)
    
    _vectors = Some(transform(rdd,index,size))
    _idf = Some(idf)
    println("object created")
    println(s"${tokens.size} tokens in dataset")
  }  // end construct
  
  def constructLabels = {
    println("construct labels")
    categoryCounts = rdd.countByKey
    _labels = Some(categoryCounts.toSeq.sortBy(-_._2).map(_._1).zipWithIndex.toMap)
    /*
    val yaml:String = categoryCounts
      .toSeq
      .sortBy(-_._2)
      .map(p => p._1+": "+p._2+"\n")
      .mkString("")
    
    write("categories.yml", yaml)
    */
  }
  
  def label(label: String):Int = {
    val labels_ = labels
    labels_(label)
  }
  
  def label(label: Int):String = {
    val inverted = labels.map(_.swap)
    inverted(label)
  }
  
  def transform(seq:Seq[String],
                index:Map[String,Int],
                size:Int): SparseVector = {
    val (indices,values) = seq.map ( word => (index(word).toInt,1) )
      .groupBy(_._1)
      .map(p => (p._1, p._2.size.toDouble))
      .toSeq
      .sortBy(_._1)
      .unzip
    new SparseVector(size, indices.toArray, values.toArray)
  }
  
  def transform(rdd: RDD[(String, Seq[String])],
                index:Map[String,Int],
                size:Int): RDD[(Int,SparseVector)] = {
    rdd.map( p => (label(p._1), transform(p._2,index,size)) )
  }
  
  def stopwords(percentage:Double):Set[String] = {
    val total= (size*percentage).toInt
    index.toSeq.sortBy(-_._2).map(_._1).take(total).toSet
  }
  
  def stopwords():Set[String] = stopwords(0.03)
  
  def withoutStopwords(percentage:Double): RDD[(String, Seq[String])] = {
    val stopwords = this.stopwords(percentage)
    rdd.mapValues(_.filter(t => ! stopwords.contains(t)))
  }
  
  def withoutStopwords(): RDD[(String, Seq[String])] = withoutStopwords(0.03)
  
  def labeled: RDD[LabeledPoint] = {
    _vectors.getOrElse{constructVectors}
    val index_ = index
    val size_ = size
    transform(rdd,index_,size_).map( p => LabeledPoint(p._1, p._2))
  }
  
  def constructWordbags = {
    val maxBags    = 100
    val bagMinSize = 100
    val bagMaxSize = 1000
    val threshold  = 1.0
    
    val labelindex = labels
    
    val input = withoutStopwords.values
    word2vec = Some(new Word2Vec())
    word2vecmodel = Some(word2vec.get.fit(input))
    val model = word2vecmodel.get
    
    val vocabulary = collection.mutable.Set(model.getVectors.keys.toArray:_*)
    println(s"vocabulary size is ${vocabulary.size}")
    val bags = collection.mutable.ArrayBuffer[collection.mutable.Set[String]]()
    
    var totalCandidates = 0
    while (vocabulary.size > bagMinSize && bags.size < maxBags){
      System.err.print("\r")
      System.err.print(s"remaining words: ${vocabulary.size}",
        s"bags: ${bags.size}",
        s"words/bag: ${totalCandidates/(bags.size+1)}"
      )
      System.err.print("          ")
      
      val word = vocabulary.head
      vocabulary remove word
      
      val synonyms = model.findSynonyms(word, bagMaxSize).filter(p=>p._2 > threshold).map(p=>p._1).toSet
      
      val candidates = vocabulary intersect synonyms
      if(candidates.size >= bagMinSize){
        candidates add word
        bags append candidates
        totalCandidates += (candidates.size+1)
      }
    }
    println("\ncreate index")
    val wordindex = collection.mutable.Map[String,Int]()
    bags.zipWithIndex.foreach{case (set,i)=>{set.foreach(word=>wordindex(word)=i)}}
    val iwordindex = collection.immutable.Map[String,Int](wordindex.toSeq:_*)
    val words = wordindex.keys.toSeq
    val vocabularyrdd = rdd.mapValues(stringSeq => stringSeq.filter(words.contains(_)))
    val vectorrdd = transform(vocabularyrdd,iwordindex,bags.size)
    val labeledrdd = vectorrdd.map(p => LabeledPoint(p._1, p._2))
    _wordbags = Some(labeledrdd)
  }
}
