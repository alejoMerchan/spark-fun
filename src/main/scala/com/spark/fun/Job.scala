package com.spark.fun

import org.apache.spark.sql.SparkSession

object Job extends  App {

  val spark = SparkSession.builder().appName("job").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  val logComcast = spark.read.json("c:/----.json")

  println("total data set to validate from ----Event: " + logComcast.count())

  val y = logComcast.filter(((logComcast.col("trickplayName").isNull and logComcast.col("trickplayName").
    isin("SKIP_FWD","SKIP_REW","SKIP_FWD_X","SKIP_REW_X")) or logComcast.col("playbackRate").
    isin(0.0,0.5,1.0,4.0,8.0,15.0,16.0,30.0,32.0,40.0,60.0,64.0,-0.5,1.0,-4.0,-8.0,-15.0,-16.0,-30.0,-32.0,-40.0,
      -60.0,-64.0))).count()

  println("total data with filter: " + y)

  logComcast.groupBy(logComcast.col("playbackRate")).count().show()

  println("toda data with playbackRate in null: " + logComcast.filter(logComcast.col("playbackRate").isNull).count())


  val logCox = spark.read.json("c:/----.json")

  println("total data set to validate from ----Event: " + logCox.count())

  val z = logCox.filter(((logCox.col("trickplayName").isNull  and logCox.col("trickplayName").
    isin("SKIP_FWD","SKIP_REW","SKIP_FWD_X","SKIP_REW_X")) or logCox.col("playbackRate").
    isin(0.0,0.5,1.0,4.0,8.0,15.0,16.0,30.0,32.0,40.0,60.0,64.0,-0.5,1.0,-4.0,-8.0,-15.0,-16.0,-30.0,-32.0,-40.0,
      -60.0,-64.0))).count()

  println("total data with filter: " + z)

  logCox.groupBy(logCox.col("playbackRate")).count().show()

  println("toda data with playbackRate in null: " + logCox.filter(logCox.col("playbackRate").isNull).count())

  spark.stop()



}
