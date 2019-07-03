package com.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Sorting {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val conf = new SparkConf().setAppName("Spark - Secondary Sort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val output = "data/WordCountOutput"

    val personRDD = sc.textFile("C:\\Users\\Lenovo\\IdeaProjects\\M2_ICP1_1\\input1.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0),k(1)), (k(2),k(3))) }
    println("pairsRDD")
    pairsRDD.foreach {
      println
    }
    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))
    println("listRDD")
    listRDD.foreach {
      println
    }

    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }

    resultRDD.saveAsTextFile(output)
    sc.stop()

  }

}
