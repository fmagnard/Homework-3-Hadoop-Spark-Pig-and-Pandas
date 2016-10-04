package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    
    // read the file
    val file = sc.textFile("hdfs://localhost:8020" + args(0))
	
	// split each document into words
    val tokenized = file.map(_.split("\t"))
    // filtered to remove weight=1
    val filtered = tokenized.filter(x => x(2).toInt > 1)


    // count node weights
    val sourceweight= filtered.map(x => (x(0), x(2).toInt*0.8))
    val targetweight = filtered.map(x => (x(1), x(2).toInt*0.2))
	val node = sourceweight.union(targetweight)
	val weightCounts = node.reduceByKey(_ + _)
 
 
    val result = weightCounts.map(x => x._1 + "\t" + x._2.toString)
    // store output on given HDFS path.
    // YOU NEED TO CHANGE THIS
    result.saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
