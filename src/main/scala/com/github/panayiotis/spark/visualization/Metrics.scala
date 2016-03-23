package com.github.panayiotis.spark.visualization

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, 
MulticlassMetrics}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer

object Metrics {
  def apply() = {new Metrics}
}

class Metrics {
  
  private val multiclassMetricsArray = new collection.mutable.ArrayBuffer[(String,MulticlassMetrics)]
  private val predictionAndLabelsArray = new collection.mutable.ArrayBuffer[(String,RDD[(Double,Double)])]
  private var labels = None: Option[Map[String,Int]]
  
  def toSeq = multiclassMetricsArray.map(p => p._2).toSeq
  
  def add(label :String, 
          predictionAndLabels: RDD[(Double,Double)]) = {
    multiclassMetricsArray.append((label,new MulticlassMetrics(predictionAndLabels)))
    predictionAndLabelsArray.append((label,predictionAndLabels))
    this
  }
  
  def add(tuple:(String, RDD[(Double,Double)])) = {
    val label = tuple._1
    val predictionAndLabels = tuple._2
    multiclassMetricsArray.append((label, new MulticlassMetrics(predictionAndLabels)))
    predictionAndLabelsArray.append((label, predictionAndLabels))
    this
  }
  
  def setLabels(labels:Map[String,Int]) = {
    this.labels = Some(labels)
    this
  }
  
  private def values(metric:MulticlassMetrics) = {
    Seq(
      metric.fMeasure,
      metric.precision,
      metric.recall,
      metric.weightedFalsePositiveRate,
      metric.weightedFMeasure,
      metric.weightedPrecision,
      metric.weightedRecall,
      metric.weightedTruePositiveRate
    ).map(d => (d * 1000).round / 1000.toDouble)
  }
  
  private def evaluationMetricsLabels = {
    Seq(
      "label",
      "fMeasure",
      "precision",
      "recall",
      "weighted false positive rate",
      "weighted fMeasure",
      "weighted precision",
      "weighted recall",
      "weighted true positive rate"
    )
  }
  
  override def toString = {
    val header = evaluationMetricsLabels.mkString("","\t","\n")
    multiclassMetricsArray.map(p => p._1 + "\t" + values(p._2).mkString("\t"))
      .mkString(header,"\n","")
  }
  
  def length = multiclassMetricsArray.length
  
  def size = multiclassMetricsArray.size
  
  private def oneVsMany = {
    val numClasses = multiclassMetricsArray.head._2.labels.max.toInt
  
    val projected: ArrayBuffer[(String, IndexedSeq[RDD[(Double, Double)]])] = predictionAndLabelsArray.map {
      case (desc, predictionAndLabels) => {
        val projected = (0 until numClasses).map {
          index => {
            predictionAndLabels.map {
              case (labelA, labelB) => {
                (if (labelA == index) 1.0 else 0.0, if (labelB == index) 1.0 else 0.0)
              }
            }
          }
        }
        (desc, projected: IndexedSeq[RDD[(Double, Double)]])
      }
    }
    projected
  }
  
  private def rocs = {
    oneVsMany.map{
      case (desc, predictionAndLabelsSeq) => {
        val rocsSeq: IndexedSeq[RDD[(Double, Double)]] = predictionAndLabelsSeq
          .map{
            m => {
              val roc = new BinaryClassificationMetrics(m).roc()
              roc
            }
          }
        (desc,rocsSeq)
      }
    }
  }
  
  def rocsjson = {
    // throws NoSuchElementException if labels is null
    val labels = this.labels.get
    val swapedLabels = labels.map(_.swap)
    rocs.map {
      case (desc, rocsSeq) => {
        val rocjson = rocsSeq.zipWithIndex.map {
          case (roc, index) => {
            val cat = swapedLabels(index)
            roc.map {
              p => {
                val x = p._1.toString
                val y = p._2.toString
                s"    [$x, $y]"
              }
            }
            .collect
            .mkString(s"""{ "name": "$cat", "values": [\n""", ",\n", "]}")
          }
        }.mkString("[", ",\n", "]")
        s"""{"desc": "$desc",\n"roc":$rocjson}"""
      }
    }.mkString("[", ",\n", "]")
  }
  
  def zeppelinEvaluationTable = s"%table ${toString}"
  
  def zeppelinRocsHtml = {
    val js  = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/roc.js")).getLines.mkString
    val css = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/roc.css")).getLines.mkString
    /*
    s"""%html
    |<div id="rocsplot"></div>
    |<style>
    |  $css
    |</style>
    |<script>
    |  $js
    |  rocsobjects = $rocsjson
    |  createrocs('#rocsplot',rocsobjects)
    |</script>
    |""".stripMargin
    */
    val html = new StringBuilder
    html.append(s"""%html\n""")
    html.append(s"""<div id="rocsplot"></div>\n""")
    html.append(s"""<style>\n$css\n</style>\n""")
    html.append(s"""<script>\n$js\n""")
    html.append(s"""var rocsobjects = $rocsjson ;\n""")
    html.append(s"""createrocs('#rocsplot',rocsobjects)\n</script>""")
    html.toString()
  }
  
  private def generateId = {
    val random = new scala.util.Random
    val alphabet="abcdefghijklmnopqrstuvwxyz"
    Stream.continually(random.nextInt(alphabet.size)).map(alphabet).take(5).mkString
  }
  
  def zeppelinRocHtml = {
    val js  = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/roc.js")).getLines.mkString
    val css = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/roc.css")).getLines.mkString
    val element = generateId
    val html = new StringBuilder
    html.append(s"""%html\n""")
    html.append(s"""<div id="$element"></div>\n""")
    html.append(s"""<style>\n$css\n</style>\n""")
    html.append(s"""<script>\n$js\n""")
    html.append(s"""var rocsobjects = $rocsjson ;\n""")
    html.append(s"""createroc('#$element',rocsobjects[0].roc,600,800)\n</script>""")
    html.toString()
  }
}
