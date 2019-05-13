package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._

object Main {
  def main(args: Array[String]) {
    val inputFile="../dblp_small.csv"
    val numAnchors = 4
    val distanceThreshold = 2
    val attrIndex = 0

    val input = new File(getClass.getResource(inputFile).getFile).getPath
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(input)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)


    val t1 = System.nanoTime
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)

    val resultSize = res.count
    println(resultSize)
    val t2 = System.nanoTime

    println((t2-t1)/(Math.pow(10,9)))

    /*
    // Compute the Levenshtein distance between two strings.
    def edit_distance(k: String, v: String): Int = {
      val m = k.size
      val n = v.size
      var distanceMatrix: Array[Array[Int]] = Array.ofDim(m + 1, n + 1)

      def min3(a: Int, b: Int, c: Int): Int = { if (a < b) { if (a < c) (a) else c } else { if (b < c) b else c } }

      for(i <- 0 to m) {
        distanceMatrix(i)(0) = i
      }
      for(j <- 0 to n) {
        distanceMatrix(0)(j) = j
      }

      for(j <- 1 to n) {
        for (i <- 1 to m) {
          if (k(i-1) == v(j-1)) {
            distanceMatrix(i)(j) = distanceMatrix(i-1)(j-1)
          } else {
            distanceMatrix(i)(j) = min3(distanceMatrix(i-1)(j) + 1,   // deletion
                                        distanceMatrix(i)(j-1) + 1,   // insertion
                                        distanceMatrix(i-1)(j-1) + 1) // substitution
          }
        }
      }
      distanceMatrix(m)(n)
    }

    // cartesian
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
                                   .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString()
                                     && edit_distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))

    println(cartesian.count)
    val t2Cartesian = System.nanoTime
    println((t2Cartesian-t1Cartesian)/(Math.pow(10,9)))

    */

  }
}
