package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

object Main {
  def main(args: Array[String]) {
    val reducers = 10

    val inputFile = "hdfs:/user/cs422-group3/test/lineorder_big.tbl"
//    val input = new File(getClass.getResource(inputFile).getFile).getPath

    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputFile)

    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    var groupingList = List("lo_suppkey", "lo_shipmode", "lo_orderdate")

    var startTime = System.nanoTime()
    var res = cb.cube(dataset, List("lo_orderkey"), "lo_revenue", "AVG")
    var estimatedTime = System.nanoTime() - startTime
    println("Test if it is working with 1 dimension")
    println(estimatedTime)
    println("---------- REAL RUN BELOW -----------")


    startTime = System.nanoTime()
    res = cb.cube(dataset, groupingList, "lo_supplycost", "AVG")
    estimatedTime = System.nanoTime() - startTime
    res.collect().foreach(println) //Print our cube
    println("AVG")
    println(estimatedTime)
//    res.saveAsTextFile("./cube")

    startTime = System.nanoTime()
    res = cb.cube_naive(dataset, groupingList, "lo_supplycost", "AVG")
    estimatedTime = System.nanoTime() - startTime
    println("AVG naive")
    println(estimatedTime)
//    res.saveAsTextFile("./cubeNaive")


    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */


    //Perform the same query using SparkSQL
    //    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
    //      .agg(sum("lo_supplycost") as "sum supplycost")
    //    q1.show


  }
}