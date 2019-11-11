package com.navi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object DocRelevance {
  val df = scala.collection.mutable.Map.empty[String, Int].withDefault(_ => 0)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val (sc, sqlContext) = createRequiredContext

    val stopWords: Set[String] = obtainStopWords(sc)

    val lines: RDD[(String, String)] = sc.wholeTextFiles("E:\\Workspaces\\spark\\Spark task\\datafiles")
    val query = obtainQueryWords(sc)

    val tfMapping = lines.map(pair => {
      val docName = pair._1
      val fileContent = pair._2.toLowerCase
      val words = fileContent
        .split("\\s+")
        .map(str=>str.replaceAll("\\p{Punct}$", ""))
        .map(str=>str.replaceAll("^\\p{Punct}", ""))
        .filter(!stopWords.contains(_))

      val wordsTF: Map[String, Int] = words.foldLeft(Map.empty[String, Int]){
        (countMap: Map[String, Int], word: String) => countMap + (word -> (countMap.getOrElse(word, 0) + 1))
      }
      calculateDF(wordsTF)
      (docName, wordsTF)
      }).collect()

    val tfidf: Array[(String, Map[String, Double])] = tfMapping.map({
      case (file, tf) =>{
        file -> calculateTFIDF(tf)
      }
    })

    val normalizeValues: Array[(String, Double)] = tfidf.map({
      rec => {
        val sqrtOfSumOfSquares: Double = math.sqrt(rec._2.values.foldLeft(0.0)(_ + Math.pow(_, 2)))
        rec._1 -> sqrtOfSumOfSquares
      }}
    )

    val normalizedTfIdf = tfidf.map({
      case (file, tfIdf) =>{
        file -> normalizeTfIdf(tfIdf, normalizeValues.find( _._1.equals(file)).get)
      }
    })


    val queryVectorMapping = normalizedTfIdf.map(rec => {
      val queryVector: Array[Double] = Array.fill(rec._2.size){0}
      rec._2.zipWithIndex.foreach{ case (rec2, idx) => {
        if(query.contains(rec2._1)){
          //queryVector(idx) =  math.log(10.toDouble / df.getOrElse(rec2._1, 1))
          queryVector(idx) =  rec2._2
        }
      }}
     rec._1 -> queryVector
    })

    val tfIdfVectorMapping = normalizedTfIdf.map(
      rec => rec._1 -> rec._2.values.toArray
    )

    val similarityMapping = tfIdfVectorMapping.map(
      rec => {
        val similarity: Double = cosineSimilarity(tfIdfVectorMapping.find(_._1.equals(rec._1)).get._2 ,
          queryVectorMapping.find(_._1.equals(rec._1)).get._2)
       rec._1 -> similarity
      }
    )
    similarityMapping.sortWith(_._2 > _._2).foreach( rec => println(s"${rec._1} -> ${rec._2}"))
  }

  private def obtainStopWords(sc: SparkContext): Set[String] = {
    sc.textFile("E:\\Workspaces\\spark\\Spark task\\stopwords.txt")
      .flatMap(line => line.split("\n"))
      .map(_.trim)
      .collect.toSet
  }

  private def obtainQueryWords(sc: SparkContext): Set[String] = {
    sc.textFile("E:\\Workspaces\\spark\\Spark task\\query.txt")
      .flatMap(line => line.split("\n"))
      .flatMap(_.split("\\s"))
      .map(_.trim.toLowerCase)
      .collect.toSet
  }

  def calculateDF(wordTf:Map[String, Int])  = {
    wordTf.foreach( tf =>
      df += (tf._1 -> (df.getOrElse(tf._1, 0) + 1)))
    }

  def calculateTFIDF(tf: Map[String, Int]): Map[String, Double] = {
    tf.map({
      case (w, cnt) => {
        val tfidf = (1 + math.log(cnt)) * math.log(10.toDouble / df.getOrElse(w, 1))
        w -> tfidf
      }
    })
  }


  def normalizeTfIdf(tfidf: Map[String, Double], tuple : (String, Double)) = {
     tfidf.map({
       case rec => {
         val normalizedTfIdf = rec._2 / tuple._2
         rec._1 -> normalizedTfIdf
       }
     })
  }
  def createRequiredContext: (SparkContext, SQLContext) = {
    val sparkConf = new SparkConf().setAppName("DocumentRelevance").set("spark.master", "local")
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate
    (sparkSession.sparkContext, sparkSession.sqlContext)
  }

  def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
    if(magnitude(x) == 0 || magnitude(y) == 0)
      0
    else dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }

  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }

  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }

}
