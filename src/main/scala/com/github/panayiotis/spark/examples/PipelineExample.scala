package com.github.panayiotis.spark.examples

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import com.github.fommil.netlib.BLAS;

object PipelineExample {
  
  case class Article(id: Long, title:String, text:String, label:String)
  
  def label(label:String):Double = {
    val labels = collection.immutable.Map("World news" -> 0, "Football" -> 1, "Business" -> 2, "Politics" -> 3, "Film" -> 4, "Media" -> 5, "Technology" -> 6, "Environment" -> 7, "Society" -> 8, "Sport" -> 9, "Books" -> 10, "US news" -> 11, "Music" -> 12, "Money" -> 13, "Life and style" -> 14, "Education" -> 15, "UK news" -> 16, "Culture" -> 17, "Art and design" -> 18, "Science" -> 19, "Travel" -> 20, "Law" -> 21, "Fashion" -> 22, "Global" -> 23, "Community" -> 24)
    def labelexception(label:String)={throw new RuntimeException(s"Can't find label i$label in labels map")}
    labels.getOrElse(label.trim,labelexception(label)).toDouble
  }
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    //println(BLAS.getInstance().getClass.getName)
    
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
  
    // Define the schema using a case class.
    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
    // you can use custom classes that implement the Product interface.
    
  
    // Create an RDD of Person objects and register it as a table.
    //println(sc.textFile("data/train.csv").map(_.split('\t')).map(a => (a(3).trim)).countByValue.toSeq.sortBy(-_._2).map(_._1).zipWithIndex.map(p => s""""${p._1}" -> ${p._2}""").mkString(", "))
    
    
    val articles = sc.textFile("data/train.csv").sample(true,0.05,1)
      .map(_.split('\t'))
      .map( p => Article( p(0).toInt, p(1), p(2),p(3)))
      .toDF()
    
    articles.registerTempTable("articles")
    
    val splits = articles.randomSplit(Array(0.7, 0.3))
    val (training, test) = (splits(0), splits(1))
    
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("allwords")
    
    val stopwords = new StopWordsRemover()
      .setInputCol("allwords")
      .setOutputCol("words")
    
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("rawFeatures")
    
    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setMinDocFreq(3)
    
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(articles)
    
    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
    
    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    
    
    // Chain indexers and tree in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopwords, hashingTF, idf, labelIndexer, dt,labelConverter))

    // Fit the pipeline to train documents.
   //val model = pipeline.fit(articles)
    
    // now we can optionally save the fitted pipeline to disk
    //model.save("/tmp/spark-logistic-regression-model")
  
    // we can also save this unfit pipeline to disk
    //pipeline.save("/tmp/unfit-lr-model")
  
    // and load it back in during production
    //val sameModel = Pipeline.load("/tmp/spark-logistic-regression-model")
  
    // Prepare test documents, which are unlabeled (id, text) tuples.
  
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(100, 1000, 100000))
      .build()
  
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
    
    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2) // Use 3+ in practice
  
    // Run cross-validation, and choose the best set of parameters.
    val model = cv.fit(training)
  
    
    
    // Make predictions on test documents.
    
    println("Prediction")
    // Make predictions.
    val predictions = model.transform(test)
    
    predictions.select("label","indexedLabel","prediction","predictedLabel","features").show(1)
    
    // Select (prediction, true label) and compute test error
    println("Evaluation")
    
    // Select (prediction, true label) and compute test error
    /*
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
    */
    val regressionEvaluator = new RegressionEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = regressionEvaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    
    //val dtModel = model.stages(3).asInstanceOf[DecisionTreeClassificationModel]
    //println("Learned classification forest model:\n" + dtModel.toDebugString)
  }
}