/**
  * First Spork Program
  * Created by parshwa on 17/1/17.
  */
package rddtry

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

object RDDTryOne {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDTryOne")
    val sc = new SparkContext(conf)

    // Ignore Info messages in Console while execution
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    // Reading the file
    val ipPath = "/home/parshwa/SparkHandsOn/war-and-peace.txt"

    // Creating an RDD and caching it
    val readRDD = sc.textFile(ipPath).cache()

    // splitRDD => RDD of Array of Strings
    val splitRDD = readRDD.map(x=>x.toLowerCase.split(" "))

    // opRDD => RDD if Strings
    val opRDD = splitRDD.flatMap(x=>x)

    // Filter out stop words
    val stopWord = List("a","the","is","an","of","are","am","at","from","and","")
    val filteredRDD = opRDD.filter(x=> !stopWord.contains(x))

    println("Total Output Word Count : " + opRDD.count())
    println("Total Filtered Word Count: " + filteredRDD.count())

    // Assignment 1: Frequency Plot
    val wordUnitRDD = filteredRDD.map(x=>(x,1))
    val wordCountRDD = wordUnitRDD
        .groupBy(x=>x._1)
        .map(x=>{
          val key = x._1
          val totalCount = x._2.size
          (key,totalCount)
        })

    //wordCountRDD.foreach(println)
    // Saving the Frequency Plot in text file
    //val writer1 = new PrintWriter(new File("WordCount.csv"))
    val wordCount = wordCountRDD.coalesce(1,true).collect()
    val writer1 = new PrintWriter(new File("WordCount.csv"))
    wordCount.foreach(x=>writer1.write(x._1+","+x._2+"\n"))

    //writer1.close()
    // Assignment 2 top 50 Percentile

    val top50Percentile = wordCountRDD
      .map(x=>(x._2,x._1))
      .top((wordCountRDD.count()/2).toInt)
    val writer2 = new PrintWriter(new File("Top50Percentile.txt" ))
    top50Percentile.foreach(x=>writer2.write(x._1+" - "+x._2+"\n"))
    writer2.close()

  }

}

