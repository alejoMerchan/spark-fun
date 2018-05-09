package com.spark.fun

import org.apache.spark.sql.SparkSession

object RddsFun extends App{

  val spark = SparkSession.builder().appName("RddsFun").master("local[*]").getOrCreate()
  val sc = spark.sparkContext


  /**
    * data_transactions have transactions of x company, this information is split in:
    *
    * transaction date # time # customer id # product id # quantity # product price
    *
    */
  val transFile = sc.textFile("C:/Users/amerchan/Desktop/desarrollo/spark-fun/src/main/resources/data_transactions.txt")

  val transData = transFile.map(_.split("#"))

  var transByCust = transData.map( trans => (trans(2).toInt,trans))

  // getting a list of costumers Ids
  println(transByCust.keys.distinct().count())

  // getting the number of transactions for customer.
  val transForCustomer = transByCust.countByKey()
  transForCustomer.map(x => println(x))

  //customer with more transactions.
  val (cid,purch) = transByCust.countByKey().toSeq.sortBy(_._2).last
  println("customer: " + cid , " purchases: " + purch)

  //getting al transactions of the user with id 53
  transByCust.lookup(53).foreach(trans => println(trans.mkString(", ")))

  //apply 5% discount to transactions with product id 25
  transByCust = transByCust.mapValues{
    trans =>
      if(trans(3).toInt == 25 && trans(4).toDouble > 1)
        trans(5) = (trans(5).toDouble * 0.95).toString
      trans
  }





  sc.stop()

}
