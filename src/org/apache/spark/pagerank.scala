package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object pagerank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .getOrCreate()

      val iters = if (args.length > 1) args(1).toInt else 10
      val lines = spark.read.textFile(args(0)).rdd
      val links = lines.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.groupByKey().cache()
      // }.distinct().groupByKey().cache()

      var ranks = links.mapValues(v => 1.0)

      var start = System.currentTimeMillis()

      for (i <- 1 to iters) {
        val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
        }
        ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
        var last = ranks.collect()
      }

      val output = ranks.collect()

      var end = System.currentTimeMillis();
      var escaped = end - start;
      System.out.println("tot escaped time: " + escaped + " ms");
      System.out.println("avg escaped time: " + escaped / iters + " ms");
      output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

      spark.stop()
  }
}

