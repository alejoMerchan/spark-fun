package com.spark.fun

import org.apache.spark.{Aggregator, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingFun extends App {

  val sparkConf = new SparkConf().setAppName("streaming-fun").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf,Seconds(10))

  val streamingText = ssc.fileStream("C:/Users/amerchan/Desktop/desarrollo/spark-fun/src/main/resources/data_transactions.txt")


  streamingText.foreachRDD{rdd =>
    if(rdd.isEmpty()) println("--- rdd empty")
    else rdd.foreach{
      x => println(x)
    }
  }

  ssc.start()
  ssc.awaitTermination()

}
